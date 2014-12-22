
# LowlaDB-Node-PostgreSQL #

> A PostgreSQL Datastore for LowlaDB on Node.js servers.

## Installation ##

```bash
$ npm install lowladb-node-postgresql --save
```

## Usage ##

Construct an instance of PostgreSqlDatastore and configure LowlaDB to use it instead of the default datastore.

```js
var app = require('express');
var lowla = require('lowladb-node');
var PostgreSqlDatastore = require('lowladb-node-postgresql');

var lowlaConfig = {
  datastore: new PostgreSqlDatastore({ postgreUrl: 'postgres://[<user>:<password>@]localhost/lowladb' })
};

lowla.configureRoutes(app, lowlaConfig);
```

The `PostgreSqlDatastore` constructor takes an optional configuration object.  The following options are supported:

```js
{
  // A node-postgres (pg) client connected to a database to use, or falsey (omitted) to use postgreUrl below instead
  db: false,

  // The PostgreSQL URL to connect to if db was not specified
  postgreUrl: 'postgres://[<user>:<password>@]localhost/lowladb',

  // Where to send log output
  logger: console
}
```

## Required Database Tables ##

The PostgreSqlDatastore does not create tables.  Your database should contain the tables your app requires.

Tables are currently assumed to be in the default 'public' schema; future releases will include additional support for schemas.

The following DDL will create a table usable by the lowladb-demo-node project:

```SQL
CREATE TABLE todos
(
  id text NOT NULL,
  _version integer,
  completed boolean,
  title text,
  CONSTRAINT pk PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE todos
  OWNER TO postgres;
```

The example uses the default postgres id as the table owner; modify as appropriate.

For lowladb-node to use PostgreSQL datastore for tracking sync events, the following tables should be defined as well:

```SQL
CREATE TABLE "lowlaSequence"
(
  _id text NOT NULL,
  value integer,
  CONSTRAINT pk_id PRIMARY KEY (_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE "lowlaSequence"
  OWNER TO postgres;
```

```SQL
CREATE TABLE "lowlaAtom"
(
  _id text NOT NULL,
  id text NOT NULL,
  sequence integer,
  version integer,
  deleted boolean,
  "clientNs" text,
  CONSTRAINT "pk_lowlaAtoms" PRIMARY KEY (_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE "lowlaAtom"
  OWNER TO postgres;
```

To reset the demo app for testing, use:

```SQL
delete from "lowlaSequence";
delete from "lowlaAtom";
delete from "todos"
```