import DataLoader from "dataloader";
import { snakeCase } from "snake-case";

import {
  sql,
  CommonQueryMethodsType,
  TaggedTemplateLiteralInvocationType,
  SqlTokenType,
} from "slonik";
import {
  PrimitiveValueExpressionType,
  TypeNameIdentifierType,
} from "slonik/dist/types";
export const createNodeLoaderClass = <TRecord, TContext = unknown>(config: {
  table: string;
  column?: Extract<keyof TRecord, string> | undefined;
  columnNameTransformer?: ((column: string) => string) | undefined;
  columnType?: TypeNameIdentifierType | SqlTokenType;
  queryFactory: (
    expressions: {
      where: SqlTokenType;
    },
    context: TContext
  ) => TaggedTemplateLiteralInvocationType<unknown>;
  typeName?: string | ((node: TRecord) => string);
}) => {
  const {
    table,
    column = "id",
    columnNameTransformer = snakeCase,
    columnType = "int4",
    queryFactory,
    typeName,
  } = config;

  return class NodeLoader extends DataLoader<
    PrimitiveValueExpressionType,
    TRecord & { __typename?: string },
    string
  > {
    constructor(
      connection: CommonQueryMethodsType,
      context: TContext,
      loaderOptions?: DataLoader.Options<
        PrimitiveValueExpressionType,
        TRecord & { __typename?: string },
        string
      >
    ) {
      super(
        async (loaderKeys) => {
          const where = sql`${sql.identifier([
            table,
            columnNameTransformer(column),
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
