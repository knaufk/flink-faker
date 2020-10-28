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

```
CREATE TABLE heros (
  name STRING,
  `power` STRING, 
  age INT
) WITH (
  'connector' = 'faker', 
  'fields.name.expression' = '#{superhero.name}',
  'fields.power.expression' = '#{superhero.power}',
  'fields.age.expression' = "#{number.numberBetween '0','1000'}"
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

Currently, `faker` this source supports the following data types:

* `CHAR` 
* `VARCHAR`
* `STRING`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `BIGINT`
* `FLOAT`
* `DOUBLE`
* `DECIMAL`
* `BOOLEAN`

## License 

Copyright © 2020 Konstantin Knauf

Distributed under Apache License, Version 2.0. 

