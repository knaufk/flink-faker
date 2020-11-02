[![Build Status](https://travis-ci.com/knaufk/flink-faker.svg?branch=master)](https://travis-ci.com/knaufk/flink-faker)

# flink-faker

flink-faker is an Apache Flink [table source](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/) 
that generates fake data based on the [Java Faker](https://github.com/DiUS/java-faker) expression 
provided for each column.

This project is inspired by [voluble](https://github.com/MichaelDrogalis/voluble). 

## Package

```shell script
mvn package
```

## Package for Ververica Platform

```shell script
mvn package
docker build . -t vvp-gateway:2.3.0-faker-0.1-SNAPSHOT
```

## Usage

### As ScanTableSource

```sql
CREATE TEMPORARY TABLE heros (
  name STRING,
  `power` STRING, 
  age INT
) WITH (
  'connector' = 'faker', 
  'fields.name.expression' = '#{superhero.name}',
  'fields.power.expression' = '#{superhero.power}',
  'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'
);

SELECT * FROM heros;
```

### As LookupTableSource

```sql
CREATE TEMPORARY TABLE location_updates (
  `character_id` INT,
  `location` STRING,
  proctime AS PROCTIME()
)
WITH (
  'connector' = 'faker', 
  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',
  'fields.location.expression' = '#{harry_potter.location}'
);

CREATE TEMPORARY TABLE characters (
  `character_id` INT,
  name STRING
)
WITH (
  'connector' = 'faker', 
  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',
  'fields.name.expression' = '#{harry_potter.characters}'
);

SELECT 
  c.character_id,
  l.location,
  c.name
FROM location_updates AS l
JOIN characters FOR SYSTEM_TIME AS OF proctime AS c
ON l.character_id = c.character_id;
```

Currently, the `faker` source supports the following data types:

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
* `TIMESTAMP`

## License 

Copyright © 2020 Konstantin Knauf

Distributed under Apache License, Version 2.0. 

