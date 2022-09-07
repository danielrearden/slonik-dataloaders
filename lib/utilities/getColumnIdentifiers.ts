import { IdentifierSqlToken, sql } from "slonik";
import { snakeCase } from "snake-case";

export const getColumnIdentifiers = <T>(
  tableAlias: string,
  columnNameTransformer: (column: string) => string = snakeCase
) => {
  return new Proxy(
    {},
    {
      get: (_target, property: string) =>
        sql.identifier([tableAlias, columnNameTransformer(property)]),
    }
  ) as Record<keyof T, IdentifierSqlToken>;
};
