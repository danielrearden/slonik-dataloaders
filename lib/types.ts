import { IdentifierSqlTokenType } from "slonik";

export type OrderDirection = "ASC" | "DESC";

export interface ColumnNamesByTable {
  [key: string]: string;
}

export type ColumnIdentifiersByTable<
  TColumnNamesByTable extends ColumnNamesByTable
> = {
  [K in keyof TColumnNamesByTable]: Record<
    TColumnNamesByTable[K],
    IdentifierSqlTokenType
  >;
};

export interface PageInfo {
  hasNextPage: boolean;
  hasPreviousPage: boolean;
  startCursor: string | null;
  endCursor: string | null;
}

export interface Connection<TEdge> {
  edges: (TEdge & { cursor: string })[];
  count: number;
  pageInfo: PageInfo;
}
