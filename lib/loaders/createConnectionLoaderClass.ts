import DataLoader from "dataloader";
import { GraphQLResolveInfo } from "graphql";
import { CommonQueryMethods, sql, QuerySqlToken, SqlToken } from "slonik";
import { z, ZodTypeAny } from "zod";
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
  where?: (identifiers: ColumnIdentifiers<TResult>) => SqlToken;
  info?: Pick<GraphQLResolveInfo, "fieldNodes" | "fragments">;
};

const SORT_COLUMN_ALIAS = "s1";
const TABLE_ALIAS = "t1";

export const createConnectionLoaderClass = <T extends ZodTypeAny>(config: {
  columnNameTransformer?: (column: string) => string;
  query: QuerySqlToken<T>;
}) => {
  const { columnNameTransformer = snakeCase, query } = config;
  const columnIdentifiers = getColumnIdentifiers<z.infer<T>>(
    TABLE_ALIAS,
    columnNameTransformer
  );

  return class ConnectionLoaderClass extends DataLoader<
    DataLoaderKey<z.infer<T>>,
    Connection<z.infer<T>>,
    string
  > {
    constructor(
      pool: CommonQueryMethods,
      dataLoaderOptions?: DataLoader.Options<
        DataLoaderKey<z.infer<T>>,
        Connection<z.infer<T>>,
        string
      >
    ) {
      super(
        async (loaderKeys) => {
          const edgesQueries: QuerySqlToken[] = [];
          const countQueries: QuerySqlToken[] = [];

          loaderKeys.forEach((loaderKey) => {
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
              ? [sql.fragment`(${where(columnIdentifiers)})`]
              : [];

            if (requestedFields.has("count")) {
              countQueries.push(
                sql.unsafe`(
                  SELECT
                  count(*) count
                  FROM (
                    ${query}
                  ) ${sql.identifier([TABLE_ALIAS])}
                  WHERE ${
                    conditions.length
                      ? sql.fragment`${sql.join(
                          conditions,
                          sql.fragment` AND `
                        )}`
                      : sql.fragment`TRUE`
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

              const selectExpressions = [
                sql.fragment`${sql.identifier([TABLE_ALIAS])}.*`,
                sql.fragment`json_build_array(${
                  orderByExpressions.length
                    ? sql.join(
                        orderByExpressions.map(([expression]) => expression),
                        sql.fragment`,`
                      )
                    : sql.fragment``
                }) ${sql.identifier([SORT_COLUMN_ALIAS])}`,
              ];

              const orderByClause = orderByExpressions.length
                ? sql.join(
                    orderByExpressions.map(
                      ([expression, direction]) =>
                        sql.fragment`${expression} ${
                          direction === (reverse ? "DESC" : "ASC")
                            ? sql.fragment`ASC`
                            : sql.fragment`DESC`
                        }`
                    ),
                    sql.fragment`,`
                  )
                : sql.fragment`TRUE`;

              if (cursor) {
                const values = fromCursor(cursor);
                conditions.push(
                  sql.fragment`(${sql.join(
                    orderByExpressions.map((_orderByExpression, outerIndex) => {
                      const expressions = orderByExpressions.slice(
                        0,
                        outerIndex + 1
                      );

                      return sql.fragment`(${sql.join(
                        expressions.map(
                          ([expression, direction], innerIndex) => {
                            let comparisonOperator = sql.fragment`=`;
                            if (innerIndex === expressions.length - 1) {
                              comparisonOperator =
                                direction === (reverse ? "DESC" : "ASC")
                                  ? sql.fragment`>`
                                  : sql.fragment`<`;
                            }

                            return sql.fragment`${expression} ${comparisonOperator} ${values[innerIndex]}`;
                          }
                        ),
                        sql.fragment` AND `
                      )})`;
                    }),
                    sql.fragment` OR `
                  )})`
                );
              }

              const whereExpression = conditions.length
                ? sql.fragment`${sql.join(conditions, sql.fragment` AND `)}`
                : sql.fragment`TRUE`;

              edgesQueries.push(
                sql.unsafe`(
                  SELECT
                    ${sql.join(selectExpressions, sql.fragment`, `)}
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

          let edgeSchema: ZodTypeAny = z.any();

          if ("shape" in query.parser) {
            edgeSchema = z
              .object({
                [SORT_COLUMN_ALIAS]: z.array(z.any()),
                ...(query.parser as any).shape,
              })
              .strict();
          }

          const countSchema = z.object({
            count: z.number(),
          });

          const [edgeResults, countResults] = await Promise.all([
            Promise.all(
              edgesQueries.map((query) => {
                return pool.any(sql.type(edgeSchema)`${query}`);
              })
            ),
            Promise.all(
              countQueries.map((query) => {
                return pool.oneFirst(sql.type(countSchema)`${query}`);
              })
            ),
          ]);

          const connections = loaderKeys.map((loaderKey, index) => {
            const { cursor, limit, reverse = false } = loaderKey;

            const edges = (edgeResults[index] ?? []).map((record) => {
              const cursorValues = new Array();

              let index = 0;
              while (true) {
                // @ts-ignore
                const value = record[SORT_COLUMN_ALIAS]?.[index];
                if (value !== undefined) {
                  cursorValues.push(value);
                  index++;
                } else {
                  break;
                }
              }

              // Stripe out `__typename`, otherwise if the connection object is returned inside a resolver,
              // GraphQL will throw an error because the typename won't match the edge type
              // @ts-ignore
              const { __typename, ...edgeFields } = record;

              return {
                ...edgeFields,
                cursor: toCursor(cursorValues),
                node: record,
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

            const count = countResults[index] ?? 0;

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
