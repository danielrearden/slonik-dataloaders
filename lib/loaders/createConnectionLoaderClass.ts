import DataLoader from "dataloader";
import { GraphQLResolveInfo } from "graphql";
import {
  CommonQueryMethods,
  sql,
  SqlToken,
  TaggedTemplateLiteralInvocation,
} from "slonik";
import { z, AnyZodObject } from "zod";
import { snakeCase } from "snake-case";
import { ColumnIdentifiers, Connection, OrderDirection } from "../types";
import {
  fromCursor,
  getColumnIdentifiers,
  getRequestedFields,
  toCursor,
} from "../utilities";

export type DataLoaderKey<TResult> = {
  cursor?: string | null;
  limit?: number | null;
  reverse?: boolean;
  orderBy?: (
    identifiers: ColumnIdentifiers<TResult>
  ) => [SqlToken, OrderDirection][];
  where?: (
    identifiers: ColumnIdentifiers<TResult>
  ) => TaggedTemplateLiteralInvocation<any>;
  info?: Pick<GraphQLResolveInfo, "fieldNodes" | "fragments">;
};

const SORT_COLUMN_ALIAS = "s1";
const TABLE_ALIAS = "t1";

export const createConnectionLoaderClass = <
  TResult extends Record<string, any>
>(config: {
  columnNameTransformer?: (column: string) => string;
  query: TaggedTemplateLiteralInvocation<TResult>;
}) => {
  const { columnNameTransformer = snakeCase, query } = config;
  const columnIdentifiers = getColumnIdentifiers<TResult>(
    TABLE_ALIAS,
    columnNameTransformer
  );

  return class ConnectionLoaderClass extends DataLoader<
    DataLoaderKey<TResult>,
    Connection<TResult>,
    string
  > {
    constructor(
      pool: CommonQueryMethods,
      dataLoaderOptions?: DataLoader.Options<
        DataLoaderKey<TResult>,
        Connection<TResult>,
        string
      >
    ) {
      super(
        async (loaderKeys) => {
          const edgesQueries: TaggedTemplateLiteralInvocation<any>[] = [];
          const countQueries: TaggedTemplateLiteralInvocation<any>[] = [];

          loaderKeys.forEach((loaderKey, index) => {
            const {
              cursor,
              info,
              limit,
              orderBy,
              reverse = false,
              where,
            } = loaderKey;

            // If a GraphQLResolveInfo object was not provided, we will assume both pageInfo and edges were requested
            const requestedFields = info
              ? getRequestedFields(info)
              : new Set(["pageInfo", "edges", "count"]);

            const conditions: SqlToken[] = where
              ? [sql`(${where(columnIdentifiers)})`]
              : [];
            const queryKey = String(index);

            const selectExpressions = [sql`${queryKey} "key"`];

            if (requestedFields.has("count")) {
              countQueries.push(
                sql`(
                  SELECT
                    ${sql.join(
                      [...selectExpressions, sql`count(*) count`],
                      sql`, `
                    )}
                  FROM (
                    ${query}
                  ) ${sql.identifier([TABLE_ALIAS])}
                  WHERE ${
                    conditions.length
                      ? sql`${sql.join(conditions, sql` AND `)}`
                      : sql`true`
                  }
                )`
              );
            }

            if (
              requestedFields.has("pageInfo") ||
              requestedFields.has("edges")
            ) {
              const orderByExpressions: [SqlToken, OrderDirection][] = orderBy
                ? orderBy(columnIdentifiers)
                : [];

              selectExpressions.push(
                sql`${sql.identifier([TABLE_ALIAS])}.*`,
                sql`json_build_array(${
                  orderByExpressions.length
                    ? sql.join(
                        orderByExpressions.map(([expression]) => expression),
                        sql`,`
                      )
                    : sql``
                }) ${sql.identifier([SORT_COLUMN_ALIAS])}`
              );

              const orderByClause = orderByExpressions.length
                ? sql.join(
                    orderByExpressions.map(
                      ([expression, direction]) =>
                        sql`${expression} ${
                          direction === (reverse ? "DESC" : "ASC")
                            ? sql`ASC`
                            : sql`DESC`
                        }`
                    ),
                    sql`,`
                  )
                : sql`true`;

              if (cursor) {
                const values = fromCursor(cursor);
                conditions.push(
                  sql`(${sql.join(
                    orderByExpressions.map((_orderByExpression, outerIndex) => {
                      const expressions = orderByExpressions.slice(
                        0,
                        outerIndex + 1
                      );

                      return sql`(${sql.join(
                        expressions.map(
                          ([expression, direction], innerIndex) => {
                            let comparisonOperator = sql`=`;
                            if (innerIndex === expressions.length - 1) {
                              comparisonOperator =
                                direction === (reverse ? "DESC" : "ASC")
                                  ? sql`>`
                                  : sql`<`;
                            }

                            return sql`${expression} ${comparisonOperator} ${values[innerIndex]}`;
                          }
                        ),
                        sql` AND `
                      )})`;
                    }),
                    sql` OR `
                  )})`
                );
              }

              const whereExpression = conditions.length
                ? sql`${sql.join(conditions, sql` AND `)}`
                : sql`true`;

              edgesQueries.push(
                sql`(
                  SELECT
                    ${sql.join(selectExpressions, sql`, `)}
                  FROM (
                    ${query}
                  ) ${sql.identifier([TABLE_ALIAS])}
                  WHERE ${whereExpression}
                  ORDER BY ${orderByClause}
                  LIMIT ${limit ? limit + 1 : null}
                )`
              );
            }
          });

          let extendedParser;

          if (query.parser) {
            const parser = query.parser as unknown as AnyZodObject;

            extendedParser = parser.extend({
              key: z.union([z.string(), z.number()]),
              s1: z.array(z.union([z.string(), z.number()])),
            });
          }

          const sqlTag = extendedParser ? sql.type(extendedParser) : sql;

          const [edgesRecords, countRecords] = await Promise.all([
            edgesQueries.length
              ? pool.any<any>(sqlTag`${sql.join(edgesQueries, sql`UNION ALL`)}`)
              : [],
            countQueries.length
              ? pool.any<any>(sql`${sql.join(countQueries, sql`UNION ALL`)}`)
              : [],
          ]);

          const connections = loaderKeys.map((loaderKey, index) => {
            const queryKey = String(index);
            const { cursor, limit, reverse = false } = loaderKey;

            const edges = edgesRecords
              .filter((record) => {
                return record.key === queryKey;
              })
              .map((record) => {
                const { key, ...rest } = record;
                const cursorValues = new Array();

                let index = 0;
                while (true) {
                  const value = record[SORT_COLUMN_ALIAS]?.[index];
                  if (value !== undefined) {
                    cursorValues.push(value);
                    index++;
                  } else {
                    break;
                  }
                }

                return {
                  ...rest,
                  cursor: toCursor(cursorValues),
                  node: rest,
                };
              });

            const slicedEdges = edges.slice(
              0,
              limit == null ? undefined : limit
            );

            if (reverse) {
              slicedEdges.reverse();
            }

            const hasMore = Boolean(edges.length > slicedEdges.length);
            const pageInfo = {
              endCursor: slicedEdges[slicedEdges.length - 1]?.cursor || null,
              hasNextPage: reverse ? !!cursor : hasMore,
              hasPreviousPage: reverse ? hasMore : !!cursor,
              startCursor: slicedEdges[0]?.cursor || null,
            };

            const count =
              countRecords.find((record) => {
                return record.key === queryKey;
              })?.count ?? 0;

            return {
              count,
              edges: slicedEdges,
              pageInfo,
            };
          });

          return connections;
        },
        {
          ...dataLoaderOptions,
          cacheKeyFn: ({
            cursor,
            info,
            limit,
            orderBy,
            reverse = false,
            where,
          }) => {
            const requestedFields = info
              ? getRequestedFields(info)
              : new Set(["pageInfo", "edges"]);

            return `${cursor}|${reverse}|${limit}|${JSON.stringify(
              orderBy?.(columnIdentifiers)
            )}|${JSON.stringify(
              where?.(columnIdentifiers)
            )}|${requestedFields.values()}`;
          },
        }
      );
    }
  };
};
