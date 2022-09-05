import DataLoader from "dataloader";
import { snakeCase } from "snake-case";

import {
  sql,
  CommonQueryMethods,
  TaggedTemplateLiteralInvocation,
  SqlToken,
  TypeNameIdentifier,
  PrimitiveValueExpression,
} from "slonik";

const TABLE_ALIAS = "t1";

export const createNodeByIdLoaderClass = <
  TRecord extends Record<string, any>
>(config: {
  column?: {
    name?: Extract<keyof TRecord, string> | undefined;
    type?: TypeNameIdentifier | SqlToken;
  };
  columnNameTransformer?: ((column: string) => string) | undefined;
  query: TaggedTemplateLiteralInvocation<TRecord>;
}) => {
  const {
    column: { name: columnName = "id", type: columnType = "int4" } = {},
    columnNameTransformer = snakeCase,
    query,
  } = config;

  return class NodeLoader extends DataLoader<
    PrimitiveValueExpression,
    TRecord & { __typename?: string },
    string
  > {
    constructor(
      pool: CommonQueryMethods,
      loaderOptions?: DataLoader.Options<
        PrimitiveValueExpression,
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

          const sqlTag = query.parser ? sql.type(query.parser) : sql;

          const records = await pool.any<any>(
            sqlTag`
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
