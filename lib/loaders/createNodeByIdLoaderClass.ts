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

const TABLE_ALIAS = "t1";

export const createNodeByIdLoaderClass = <TRecord>(config: {
  column?: {
    name?: Extract<keyof TRecord, string> | undefined;
    type?: TypeNameIdentifierType | SqlTokenType;
  };
  columnNameTransformer?: ((column: string) => string) | undefined;
  query: TaggedTemplateLiteralInvocationType<TRecord>;
}) => {
  const {
    column: { name: columnName = "id", type: columnType = "int4" } = {},
    columnNameTransformer = snakeCase,
    query,
  } = config;

  return class NodeLoader extends DataLoader<
    PrimitiveValueExpressionType,
    TRecord & { __typename?: string },
    string
  > {
    constructor(
      pool: CommonQueryMethodsType,
      loaderOptions?: DataLoader.Options<
        PrimitiveValueExpressionType,
        TRecord & { __typename?: string },
        string
      >
    ) {
      super(
        async (loaderKeys) => {
          const where = sql`${sql.identifier([
            TABLE_ALIAS,
            columnNameTransformer(columnName),
          ])} = ANY(${sql.array(loaderKeys, columnType)})`;
          const records = await pool.any<any>(
            sql`
              SELECT *
              FROM (
                ${query}
              ) ${sql.identifier([TABLE_ALIAS])}
              WHERE ${where}
            `
          );

          const recordsByLoaderKey = loaderKeys.map((value) => {
            const record = records.find((record) => {
              return String(record[columnName]) === String(value);
            });

            if (record) {
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
