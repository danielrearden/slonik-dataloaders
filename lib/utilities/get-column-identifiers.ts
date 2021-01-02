import { sql } from "slonik";
import { snakeCase } from "snake-case";
import { ColumnIdentifiersByTable, ColumnNamesByTable } from "../types";

export const getColumnIdentifiers = <TColumns extends ColumnNamesByTable>(
  tableAlias: string,
  columnNameTransformer: (column: string) => string = snakeCase
): ColumnIdentifiersByTable<TColumns> => {
  return new Proxy(
    {},
    {
      get: (_target, property: string) =>
        sql.identifier([tableAlias, columnNameTransformer(property)]),
    }
  ) as ColumnIdentifiersByTable<TColumns>;
};
