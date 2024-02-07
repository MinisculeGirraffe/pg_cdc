# pg_cdc

## How it works

[Logical replication](https://www.postgresql.org/docs/current/protocol-logical-replication.html) is used to ship changes in real time to this program as if we were a secondary database. We parse the postgres wire format and then map it to a generic representation that is serializable that we then replicate to various message queues.

The database is typically the constraint for scaling web applications. Logical replication is practically free compared to other methods of real-time updates such as trigger subscriptions. The WAL is functionally a stream of database changes, already so it's no extra work for the database then sending it over a network socket.

## Why would I want this

Being able to subscribe to real time database changes is useful for a variety of applications. For example, sending a confirmation email to a user when a row is added to the orders table.

Additionally, for the purpose of why I wrote this software, [GraphQL subscriptions](https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md) for real time web applications. Often, graphql subscriptions are often implemented at the application layer by hooking into incoming mutation requests. Looking at you AWS AppSync. This is annoying for multiple reasons. It means we can't notify clients of changes unless the update went through the GraphQL API. Lame.


## FAQ

- What about existing solutions such as [debezium](https://debezium.io/).

It's better software with more features and you should use it if you have the need for it. I wrote this to use on side-projects I build for fun/learning.

Being written that it's Java it used over 1 GiB of RAM, which is a non-starter for hosting cheaply. It also doesn't talk directly to Nats, which I wanted to use for the same reasons as described.

# Requirement

- postgresql > 10
- "wal_level=logical"
- max_replication_slots to at least 1
- An existing publication created in your database.

https://www.postgresql.org/docs/current/sql-createpublication.html