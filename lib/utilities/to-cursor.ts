import { QueryResultRowColumnType } from "slonik";

export const toCursor = (ids: QueryResultRowColumnType[]): string => {
  return Buffer.from(JSON.stringify(ids)).toString("base64");
};
