# `slonik-dataloaders`

`slonik-dataloaders` is a set of utilities for creating [DataLoaders](https://github.com/graphql/dataloader) using [Slonik](https://github.com/gajus/slonik). These DataLoaders abstract away some of the complexity of working with cursor-style pagination when working with a SQL database, while still maintaining the flexibility that comes with writing raw SQL statements.

### `createNodeLoaderClass`

Example usage

```ts
const UserByIdLoader = createNodeLoaderClass<User>({
  table: "user",
  column: "id",
  queryFactory: ({ where }) => sql`
    SELECT
      *
    FROM user
    WHERE ${where}
  `,
});
const pool = createPool("postgresql://");
const loader = new UserByIdLoader(pool, {});
const user = await loader.load(99);
```

Example usage with context

```ts
type Context = {
  currentUserId: number;
};

const MessageByIdLoader = createNodeLoaderClass<Message, Context>({
  table: "message",
  column: "id",
  queryFactory: ({ where }, { currentUserId }) => sql`
    SELECT
      *
    FROM message
    WHERE ${where} AND message.recipient_user_id = ${currentUserId}
  `,
});
const pool = createPool("postgresql://");
const loader = new MessageByIdLoader(pool, { currentUserId });
const message = await loader.load(99);
```

### `createConnectionLoaderClass`

Example usage

```ts
const UserConnectionLoader = createConnectionLoaderClass<
  { node: User },
  { id: number }
>({
  tables: {
    node: "user",
  },
  queryFactory: ({ limit, orderBy, select, where }) => sql`
    SELECT
      ${select},
      id
    FROM user
    WHERE ${where}
    ORDER BY ${orderBy}
    LIMIT ${limit}
  `,
});
const pool = createPool("postgresql://");
const loader = new UserByIdLoader(pool, {});
const connection = await loader.load({
  where: ({ node: { firstName } }) => sql`${firstName} = 'Susan'`,
  orderBy: ({ node: { firstName } }) => [[firstName, "ASC"]],
});
```

- Each loader specifies a map of aliases to table names (the `tables` property). The aliases are used inside the `where` and `orderBy` expression factories. These factory functions allow for type-safe loader usage and abstract away the actual table name or alias used inside the SQL query. The choice of `node` for the alias above is completely arbitrary -- we could have used `user` or any other name.
- The first type parameter of the function (`{ node: User }` in the above example) specifies the type associated with each table. This in turn allows for type-checking and auto-completion of the column names available as parameters inside the `where` and `orderBy` functions. The types passed in here should match the columns of the tables; however, the types can use camel-cased property names (any such names will be converted to snake case column names under the hood).
- The second type parameter of the function (`{ id: number }` in the above example) specifies the properties returned on each edge. Here, we're only selecting the `id`. The edge objects can include any number of properties, but should typically only return the id of the associated node, which can then be fetched separately using an appropriate NodeLoader.

Usage example with forward pagination

```ts
const connection = await loader.load({
  orderBy: ({ node: { firstName } }) => [[firstName, "ASC"]],
  limit: first,
  cursor: after,
});
```

Usage example with backward pagination

```ts
const connection = await loader.load({
  orderBy: ({ node: { firstName } }) => [[firstName, "ASC"]],
  limit: last,
  cursor: before,
  reverse: true,
});
```

Example usage with context

```ts
type Context = {
  currentUserId: number;
};

const UserConnectionLoader = createConnectionLoaderClass<
  { node: User },
  { id: number },
  Context
>({
  tables: {
    node: "message",
  },
  queryFactory: ({ limit, orderBy, select, where }, { currentUserId }) => sql`
    SELECT
      ${select},
      id
    FROM message
    WHERE ${where} AND recipient_user_id = ${currentUserId}
    ORDER BY ${orderBy}
    LIMIT ${limit}
  `,
});
const pool = createPool("postgresql://");
const loader = new UserByIdLoader(pool, { currentUserId });
const connection = await loader.load({});
```
