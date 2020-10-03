[![Build Status](https://travis-ci.com/knaufk/flink-faker.svg?branch=master)](https://travis-ci.com/knaufk/flink-faker)

# flink-faker

flink-faker is an Apache Flink [table source](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/) 
that generates fake data based on the [Java Faker](https://github.com/DiUS/java-faker) expression 
provided for each column.

This project is inspired by [voluble](https://github.com/MichaelDrogalis/voluble). 

## Build

```
mvn package
```

## Usage

At this point the source only supports `STRING`, `CHAR` and `VARCHAR` columns.

```
CREATE TABLE customers (
  first_name STRING,
  last_name STRING, 
  title STRING, 
  favorite_beer STRING
)
WITH (
  'connector' = 'faker', 
  'fields.first_name.expression' = '#{name._first_name}',
  'fields.last_name.expression' = '#{name.last_name}',
  'fields.title.expression' = '#{name.title}',
  'fields.favorite_beer.expression' = '#{beer.name}'
);
```

```
CREATE TABLE harrypotter (
  `character` STRING,
  quote STRING
)
WITH (
  'connector' = 'faker', 
  'fields.character.expression' = '#{harry_potter.characters}',
  'fields.quote.expression' = '#{harry_potter.quote}'
);
```

## License 

Copyright © 2020 Konstantin Knauf

Distributed under Apache License, Version 2.0. 

