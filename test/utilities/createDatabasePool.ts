import {
  createPool,
  type Interceptor,
  type QueryResultRow,
  SchemaValidationError,
} from "slonik";
import { createQueryLoggingInterceptor } from "slonik-interceptor-query-logging";

type SerializableValue =
  | SerializableValue[]
  | boolean
  | number
  | string
  | readonly SerializableValue[]
  | {
      [key: string]: SerializableValue;
    }
  | null;

export const sanitizeObject = (
  object: Record<string, unknown>
): SerializableValue => {
  return JSON.parse(JSON.stringify(object));
};

const createResultParserInterceptor = (): Interceptor => {
  return {
    transformRow: (executionContext, actualQuery, row) => {
      const { resultParser } = executionContext;

      // @ts-expect-error _any is not exposed through the zod typings, but does exist on ZodTypeAny
      if (!resultParser || resultParser._any) {
        return row;
      }

      const validationResult = resultParser.safeParse(row);

      if (validationResult.success !== true) {
        console.log("Failed validation!");
        console.log(validationResult.error);
        console.log(sanitizeObject(row));

        throw new SchemaValidationError(
          actualQuery,
          sanitizeObject(row),
          validationResult.error.issues
        );
      }

      return validationResult.data as QueryResultRow;
    },
  };
};

export const createDatabasePool = () => {
  return createPool(process.env.POSTGRES_DSN || "", {
    interceptors: [
      createResultParserInterceptor(),
      createQueryLoggingInterceptor(),
    ],
  });
};
