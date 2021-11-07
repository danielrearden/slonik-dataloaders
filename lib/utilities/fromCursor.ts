import { QueryResultRowColumnType } from "slonik";

export const fromCursor = (cursor: string): QueryResultRowColumnType[] => {
  return JSON.parse(Buffer.from(cursor, "base64").toString());
};
