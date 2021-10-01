import DataLoader from "dataloader";
import { GraphQLResolveInfo } from "graphql";
import {
  CommonQueryMethodsType,
  sql,
  SqlTokenType,
  TaggedTemplateLiteralInvocationType,
} from "slonik";
import {
  ColumnIdentifiersByTable,
  ColumnNamesByTable,
  Connection,
  OrderDirection,
} from "../types";
import {
  fromCursor,
  getColumnIdentifiers,
  getRequestedFields,
  toCursor,
} from "../utilities";

export type DataLoaderKey<TTableColumns extends ColumnNamesByTable> = {
  cursor?: string | null;
  limit?: number | null;
  reverse?: boolean;
  orderBy?: (
    identifiers: ColumnIdentifiersByTable<TTableColumns>
  ) => [SqlTokenType, OrderDirection][];
  where?: (
    identifiers: ColumnIdentifiersByTable<TTableColumns>
  ) => TaggedTemplateLiteralInvocationType<unknown>;
  info?: Pick<GraphQLResolveInfo, "fieldNodes" | "fragments">;
};

export const createConnectionLoaderClass = <
  TTableColumns extends ColumnNamesByTable,
  TEdge = { id: number },
  TContext = unknown
>(config: {
  tables: { [P in keyof TTableColumns]: string };
  queryFactory: (
    expressions: {
      limit: number | null;
      orderBy: SqlTokenType;
      select: SqlTokenType;
      where: SqlTokenType;
    },
    context: TContext
  ) => TaggedTemplateLiteralInvocationType<unknown>;
  columnNameTransformer?: (column: string) => string;
}) => {
  const { tables, queryFactory, columnNameTransformer } = config;
  const columnIdentifiersByTable = Object.keys(tables).reduce((acc, key) => {
    acc[key] = getColumnIdentifiers(tables[key], columnNameTransformer);

    return acc;
  }, {} as any);

  return class ConnectionLoaderClass extends DataLoader<
    DataLoaderKey<TTableColumns>,
    Connection<TEdge>,
    string
  > {
    constructor(
      connection: CommonQueryMethodsType,
      context: TContext,
      dataLoaderOptions?: DataLoader.Options<
        DataLoaderKey<TTableColumns>,
        Connection<TEdge>,
        string
      >
    ) {
      super(
        async (loaderKeys) => {
          const edgesQueries: TaggedTemplateLiteralInvocationType<unknown>[] = [];
          const countQueries: TaggedTemplateLiteralInvocationType<unknown>[] = [];

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
              : new Set(["pageInfo", "edges"]);

            const conditions: SqlTokenType[] = where
              ? [where(columnIdentifiersByTable)]
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
                    ${queryFactory(
                      {
                        limit: null,
                        orderBy: sql`true`,
                        select: sql`null`,
                        where: conditions.length
                          ? sql`${sql.join(conditions, sql` AND `)}`
                          : sql`true`,
                      },
                      context
                    )}
                  ) count_subquery
                )`
              );
            }

            if (
              requestedFields.has("pageInfo") ||
              requestedFields.has("edges")
            ) {
              const orderByExpressions: [
                SqlTokenType,
                OrderDirection
              ][] = orderBy ? orderBy(columnIdentifiersByTable) : [];

              selectExpressions.push(
                sql`json_build_array(${
                  orderByExpressions.length
                    ? sql.join(
                        orderByExpressions.map(([expression]) => expression),
                        sql`,`
                      )
                    : sql``
                }) "o"`
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
                sql`(${queryFactory(
                  {
                    limit: limit ? limit + 1 : null,
                    orderBy: orderByClause,
                    select: sql.join(selectExpressions, sql`, `),
                    where: whereExpression,
                  },
                  context
                )})`
              );
            }
          });

          const [edgesRecords, countRecords] = await Promise.all([
            edgesQueries.length
              ? connection.any<any>(
                  sql`${sql.join(edgesQueries, sql`UNION ALL`)}`
                )
              : [],
            countQueries.length
              ? connection.any<any>(
                  sql`${sql.join(countQueries, sql`UNION ALL`)}`
                )
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

                const edge = rest as TEdge;
                const cursorValues = new Array();

                let index = 0;
                while (true) {
                  const value = record["o"]?.[index];
                  if (value !== undefined) {
                    cursorValues.push(value);
                    index++;
                  } else {
                    break;
                  }
                }

                return {
                  cursor: toCursor(cursorValues),
                  ...edge,
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
          cache: false,
        }
      );
    }
  };
};
