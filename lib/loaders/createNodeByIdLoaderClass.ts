import DataLoader from "dataloader";
import { snakeCase } from "snake-case";
import {
  sql,
  CommonQueryMethods,
  SqlToken,
  QuerySqlToken,
  TypeNameIdentifier,
  PrimitiveValueExpression,
} from "slonik";
import { z, type ZodTypeAny } from "zod";

const TABLE_ALIAS = "t1";

export const createNodeByIdLoaderClass = <T extends ZodTypeAny>(config: {
  column?: {
    name?: Extract<keyof z.infer<T>, string> | undefined;
    type?: TypeNameIdentifier | SqlToken;
  };
  columnNameTransformer?: ((column: string) => string) | undefined;
  query: QuerySqlToken<T>;
}) => {
  const {
    column: { name: columnName = "id", type: columnType = "int4" } = {},
    columnNameTransformer = snakeCase,
    query,
  } = config;

  return class NodeLoader extends DataLoader<
    PrimitiveValueExpression,
    z.infer<T>,
    string
  > {
    constructor(
      pool: CommonQueryMethods,
      loaderOptions?: DataLoader.Options<
        PrimitiveValueExpression,
        z.infer<T>,
        string
      >
    ) {
      super(
        async (loaderKeys) => {
          const where = sql.fragment`${sql.identifier([
            TABLE_ALIAS,
            columnNameTransformer(columnName),
          ])} = ANY(${sql.array(loaderKeys, columnType)})`;

          const records = await pool.any<any>(
            sql.type(query.parser)`
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
