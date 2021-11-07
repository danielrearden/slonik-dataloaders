import { IdentifierSqlTokenType } from "slonik";

export type OrderDirection = "ASC" | "DESC";

export type ColumnIdentifiers<TResult> = Record<
  keyof TResult,
  IdentifierSqlTokenType
>;

export interface PageInfo {
  hasNextPage: boolean;
  hasPreviousPage: boolean;
  startCursor: string | null;
  endCursor: string | null;
}

export interface Connection<TResult> {
  edges: (TResult & { node: TResult; cursor: string })[];
  count: number;
  pageInfo: PageInfo;
}
