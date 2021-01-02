import { createPool, DatabasePoolType, sql } from "slonik";
import { createConnectionLoaderClass } from "../../lib";

type Bar = {
  id: number;
  uid: string;
  value: string;
};

const BarConnectionLoader = createConnectionLoaderClass<
  { node: keyof Bar },
  { id: number }
>({
  tables: {
    node: "test_table_bar",
  },
  queryFactory: ({ limit, orderBy, select, where }) => sql`
    SELECT
      ${select},
      id
    FROM test_table_bar
    WHERE ${where}
    ORDER BY ${orderBy}
    LIMIT ${limit}
  `,
});

describe("createConnectionLoaderClass", () => {
  let pool: DatabasePoolType;

  beforeAll(async () => {
    pool = createPool(process.env.POSTGRES_DSN || "");

    await pool.query(sql`
      CREATE TABLE IF NOT EXISTS test_table_bar (
        id integer NOT NULL PRIMARY KEY,
        uid text NOT NULL,
        value text NOT NULL
      );

      INSERT INTO test_table_bar
        (id, uid, value)
      VALUES
        (1, 'z', 'aaa'),
        (2, 'y', 'aaa'),
        (3, 'x', 'bbb'),
        (4, 'w', 'bbb'),
        (5, 'v', 'ccc'),
        (6, 'u', 'ccc'),
        (7, 't', 'ddd'),
        (8, 's', 'ddd'),
        (9, 'r', 'eee');
    `);
  });

  afterAll(async () => {
    await pool.query(sql`
      DROP TABLE IF EXISTS test_table_bar;
    `);

    await pool.end();
  });

  it("loads all records with no additional options", async () => {
    const loader = new BarConnectionLoader(pool, {});
    const result = await loader.load({});

    expect(result).toMatchObject({
      pageInfo: {
        startCursor: result.edges[0].cursor,
        endCursor: result.edges[8].cursor,
        hasNextPage: false,
        hasPreviousPage: false,
      },
    });
    expect(result.edges).toHaveLength(9);
  });

  it("loads records in ascending order", async () => {
    const loader = new BarConnectionLoader(pool, {});
    const result = await loader.load({
      orderBy: ({ node: { uid } }) => [[uid, "ASC"]],
    });

    expect(result.edges[0].id).toEqual(9);
    expect(result.edges[8].id).toEqual(1);
  });

  it("loads records in descending order", async () => {
    const loader = new BarConnectionLoader(pool, {});
    const result = await loader.load({
      orderBy: ({ node: { uid } }) => [[uid, "DESC"]],
    });

    expect(result.edges[0].id).toEqual(1);
    expect(result.edges[8].id).toEqual(9);
  });

  it("loads records with multiple order by expressions", async () => {
    const loader = new BarConnectionLoader(pool, {});
    const result = await loader.load({
      orderBy: ({ node: { uid, value } }) => [
        [value, "DESC"],
        [uid, "DESC"],
      ],
    });

    expect(result.edges[0].id).toEqual(9);
    expect(result.edges[8].id).toEqual(2);
  });

  it("loads records with complex order by expression", async () => {
    const loader = new BarConnectionLoader(pool, {});
    const result = await loader.load({
      orderBy: ({ node: { uid } }) => [[sql`upper(${uid})`, "ASC"]],
    });

    expect(result.edges[0].id).toEqual(9);
    expect(result.edges[8].id).toEqual(1);
  });

  it("loads records with where expression", async () => {
    const loader = new BarConnectionLoader(pool, {});
    const result = await loader.load({
      where: ({ node: { value } }) => sql`upper(${value}) = 'EEE'`,
    });

    expect(result.edges).toHaveLength(1);
    expect(result.edges[0].id).toEqual(9);
  });

  it("paginates through the records forwards", async () => {
    const loader = new BarConnectionLoader(pool, {});
    const firstResult = await loader.load({
      orderBy: ({ node: { uid } }) => [[uid, "ASC"]],
      limit: 4,
    });

    expect(firstResult.edges).toHaveLength(4);
    expect(firstResult.edges[0].id).toEqual(9);
    expect(firstResult.edges[3].id).toEqual(6);
    expect(firstResult.pageInfo).toMatchObject({
      hasPreviousPage: false,
      hasNextPage: true,
    });

    const secondResult = await loader.load({
      orderBy: ({ node: { uid } }) => [[uid, "ASC"]],
      limit: 4,
      cursor: firstResult.pageInfo.endCursor,
    });

    expect(secondResult.edges).toHaveLength(4);
    expect(secondResult.edges[0].id).toEqual(5);
    expect(secondResult.edges[3].id).toEqual(2);
    expect(secondResult.pageInfo).toMatchObject({
      hasPreviousPage: true,
      hasNextPage: true,
    });

    const thirdResult = await loader.load({
      orderBy: ({ node: { uid } }) => [[uid, "ASC"]],
      limit: 4,
      cursor: secondResult.pageInfo.endCursor,
    });

    expect(thirdResult.edges).toHaveLength(1);
    expect(thirdResult.edges[0].id).toEqual(1);
    expect(thirdResult.pageInfo).toMatchObject({
      hasPreviousPage: true,
      hasNextPage: false,
    });

    const fourthResult = await loader.load({
      orderBy: ({ node: { uid } }) => [[uid, "ASC"]],
      limit: 4,
      cursor: thirdResult.pageInfo.endCursor,
    });

    expect(fourthResult.edges).toHaveLength(0);
    expect(fourthResult.pageInfo).toMatchObject({
      hasPreviousPage: true,
      hasNextPage: false,
    });
  });

  it("paginates through the records backwards", async () => {
    const loader = new BarConnectionLoader(pool, {});
    const firstResult = await loader.load({
      orderBy: ({ node: { value, uid } }) => [
        [value, "ASC"],
        [uid, "ASC"],
      ],
      limit: 4,
      reverse: true,
    });

    expect(firstResult.edges).toHaveLength(4);
    expect(firstResult.edges[0].id).toEqual(5);
    expect(firstResult.edges[3].id).toEqual(9);
    expect(firstResult.pageInfo).toMatchObject({
      hasPreviousPage: true,
      hasNextPage: false,
    });

    const secondResult = await loader.load({
      orderBy: ({ node: { value, uid } }) => [
        [value, "ASC"],
        [uid, "ASC"],
      ],
      limit: 4,
      cursor: firstResult.pageInfo.startCursor,
      reverse: true,
    });

    expect(secondResult.edges).toHaveLength(4);
    expect(secondResult.edges[0].id).toEqual(1);
    expect(secondResult.edges[3].id).toEqual(6);
    expect(secondResult.pageInfo).toMatchObject({
      hasPreviousPage: true,
      hasNextPage: true,
    });

    const thirdResult = await loader.load({
      orderBy: ({ node: { value, uid } }) => [
        [value, "ASC"],
        [uid, "ASC"],
      ],
      limit: 4,
      cursor: secondResult.pageInfo.startCursor,
      reverse: true,
    });

    expect(thirdResult.edges).toHaveLength(1);
    expect(thirdResult.edges[0].id).toEqual(2);
    expect(thirdResult.pageInfo).toMatchObject({
      hasPreviousPage: false,
      hasNextPage: true,
    });

    const fourthResult = await loader.load({
      orderBy: ({ node: { value, uid } }) => [
        [value, "ASC"],
        [uid, "ASC"],
      ],
      limit: 4,
      cursor: thirdResult.pageInfo.startCursor,
      reverse: true,
    });

    expect(fourthResult.edges).toHaveLength(0);
    expect(fourthResult.pageInfo).toMatchObject({
      hasPreviousPage: false,
      hasNextPage: true,
    });
  });

  it("batches and caches loaded records", async () => {
    const loader = new BarConnectionLoader(pool, {});
    const poolAnySpy = jest.spyOn(pool, "any");
    const results = await Promise.all([
      loader.load({
        orderBy: ({ node: { uid } }) => [[uid, "ASC"]],
      }),
      loader.load({
        orderBy: ({ node: { uid } }) => [[uid, "DESC"]],
      }),
    ]);

    expect(poolAnySpy).toHaveBeenCalledTimes(1);
    expect(results[0].edges[0].id).toEqual(9);
    expect(results[0].edges[8].id).toEqual(1);
    expect(results[1].edges[0].id).toEqual(1);
    expect(results[1].edges[8].id).toEqual(9);
  });

  it("loads records based on context", async () => {
    const BarConnectionWithContextLoader = createConnectionLoaderClass<
      { node: keyof Bar },
      { id: number },
      { favoriteId: number }
    >({
      tables: {
        node: "test_table_bar",
      },
      queryFactory: ({ limit, orderBy, select, where }, { favoriteId }) => sql`
        SELECT
          ${select},
          id
        FROM test_table_bar
        WHERE ${where} AND id = ${favoriteId}
        ORDER BY ${orderBy}
        LIMIT ${limit}
      `,
    });
    const loader = new BarConnectionWithContextLoader(pool, { favoriteId: 2 });
    const results = await loader.load({});

    expect(results.edges).toHaveLength(1);
    expect(results.edges[0].id).toEqual(2);
  });
});
