import { createPool, DatabasePoolType, sql } from "slonik";
// @ts-ignore
import { createQueryLoggingInterceptor } from "slonik-interceptor-query-logging";
import { createNodeLoaderClass } from "../../lib";

type Foo = {
  id: number;
  uid: string;
};

const FooByIdLoader = createNodeLoaderClass<Foo>({
  table: "test_table_foo",
  column: "id",
  queryFactory: ({ where }) => sql`
    SELECT
      *
    FROM test_table_foo
    WHERE ${where}
  `,
  typeName: "Foo",
});

describe("createRecordByUniqueColumnLoader", () => {
  let pool: DatabasePoolType;

  beforeAll(async () => {
    pool = createPool(process.env.POSTGRES_DSN || "", {
      interceptors: [createQueryLoggingInterceptor()],
    });

    await pool.query(sql`
      CREATE TABLE IF NOT EXISTS test_table_foo (
        id integer NOT NULL PRIMARY KEY,
        uid text NOT NULL
      );

      INSERT INTO test_table_foo
        (id, uid)
      VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c');
    `);
  });

  afterAll(async () => {
    await pool.query(sql`
      DROP TABLE IF EXISTS test_table_foo;
    `);

    await pool.end();
  });

  it("loads record by numeric column", async () => {
    const loader = new FooByIdLoader(pool, {});
    const result = await loader.load(2);

    expect(result).toMatchObject({ id: 2, uid: "b", __typename: "Foo" });
  });

  it("returns null when a match can't be found", async () => {
    const loader = new FooByIdLoader(pool, {});
    const result = await loader.load(999);

    expect(result).toEqual(null);
  });

  it("batches and caches loaded records", async () => {
    const loader = new FooByIdLoader(pool, {});
    const poolAnySpy = jest.spyOn(pool, "any");
    const results = await Promise.all([loader.load(3), loader.load(2)]);

    expect(poolAnySpy).toHaveBeenCalledTimes(1);
    expect(results).toMatchObject([
      { id: 3, uid: "c" },
      { id: 2, uid: "b" },
    ]);
  });

  it("loads record by text column", async () => {
    const FooByUidLoader = createNodeLoaderClass<Foo>({
      table: "test_table_foo",
      column: "uid",
      columnType: "text",
      queryFactory: ({ where }) => sql`
        SELECT
          *
        FROM test_table_foo
        WHERE ${where}
      `,
      typeName: "Foo",
    });
    const loader = new FooByUidLoader(pool, {});
    const result = await loader.load("b");

    expect(result).toMatchObject({ id: 2, uid: "b" });
  });

  it("loads records based on context", async () => {
    const FooWithContextLoader = createNodeLoaderClass<
      Foo,
      { favoriteId: number }
    >({
      table: "test_table_foo",
      column: "id",
      queryFactory: ({ where }, { favoriteId }) => sql`
        SELECT
          *
        FROM test_table_foo
        WHERE ${where} AND id = ${favoriteId}
      `,
      typeName: "Foo",
    });
    const loader = new FooWithContextLoader(pool, { favoriteId: 2 });
    const result = await loader.load(1);

    expect(result).toEqual(null);
  });

  it("loads records with __typename when typeName option is a function", async () => {
    const FooWithTypeNameLoader = createNodeLoaderClass<Foo>({
      table: "test_table_foo",
      column: "id",
      queryFactory: ({ where }) => sql`
        SELECT
          *
        FROM test_table_foo
        WHERE ${where}
      `,
      typeName: () => "Foo2",
    });
    const loader = new FooWithTypeNameLoader(pool, {});
    const result = await loader.load(1);

    expect(result?.__typename).toEqual("Foo2");
  });
});
