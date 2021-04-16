import DataLoader from "dataloader";

import {
  sql,
  DatabasePoolType,
  TaggedTemplateLiteralInvocationType,
  SqlTokenType,
} from "slonik";
import {
  PrimitiveValueExpressionType,
  TypeNameIdentifierType,
} from "slonik/dist/types";
export const createNodeLoaderClass = <TRecord, TContext = unknown>(config: {
  table: string;
  column: string;
  columnType?: TypeNameIdentifierType | SqlTokenType;
  queryFactory: (
    expressions: {
      where: SqlTokenType;
    },
    context: TContext
  ) => TaggedTemplateLiteralInvocationType<unknown>;
  typeName?: string | ((node: TRecord) => string);
}) => {
  const { table, column, columnType = "int4", queryFactory, typeName } = config;

  return class NodeLoader extends DataLoader<
    PrimitiveValueExpressionType,
    (TRecord & { __typename?: string }) | null,
    string
  > {
    constructor(
      connection: DatabasePoolType,
      context: TContext,
      loaderOptions?: DataLoader.Options<
        PrimitiveValueExpressionType,
        (TRecord & { __typename?: string }) | null,
        string
      >
    ) {
      super(
        async (loaderKeys) => {
          const where = sql`${sql.identifier([
            table,
            column,
          ])} = ANY(${sql.array(loaderKeys, columnType)})`;
          const records = await connection.any<any>(
            queryFactory({ where }, context)
          );

          const recordsByLoaderKey = loaderKeys.map((value) => {
            const record = records.find((record) => {
              return String(record[column]) === String(value);
            });

            if (record) {
              if (typeName) {
                record.__typename =
                  typeof typeName === "function" ? typeName(record) : typeName;
              }
              return record;
            }

            return null;
          });

          return recordsByLoaderKey;
        },
        {
          ...loaderOptions,
          cacheKeyFn: (key) => String(key),
        }
      );
    }
  };
};
