![Build Status](https://github.com/knaufk/flink-faker/actions/workflows/ci.yml/badge.svg?branch=master)

# flink-faker

flink-faker is an Apache Flink [table source](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/)
that generates fake data based on the [Data Faker](https://github.com/datafaker-net/datafaker) expression
provided for each column.

Checkout this [demo web application](https://java-faker.herokuapp.com/) for some example Java Faker
(fully compatible with Data Faker) expressions and [Data Faker documentation](https://www.datafaker.net/documentation/providers/).

This project is inspired by [voluble](https://github.com/MichaelDrogalis/voluble).

## Package

```shell script
mvn clean package
```

## Compatibility Matrix

| Flink Version | flink-faker Version |
|---------------|---------------------|
| 1.11          | 0.1.x - 0.4.x       |
| 1.12          | 0.1.x - 0.4.x       |
| 1.13          | 0.1.x - 0.4.x       |
| 1.14          | 0.1.x - 0.4.x       |
| 1.15          | 0.5.0               |
| 1.16          | 0.5.1+              |
| 1.17          | 0.5.1+              |

There are no automated tests that check this compatibility. So, please treat this table as "best knowledge". 
If you notice any incompatibilities please open an issue.

## Using flink-faker with the Flink SQL Client

1. Download Flink from the [Apache Flink website](https://flink.apache.org/downloads.html).
2. Download the flink-faker JAR from the [Releases](https://github.com/knaufk/flink-faker/releases) page (or [build it yourself](#package)).
3. Put the downloaded jars under `lib/`.
4. (Re)Start a [Flink cluster](https://ci.apache.org/projects/flink/flink-docs-stable/docs/try-flink/local_installation/#step-2-start-a-cluster).
5. (Re)Start the [Flink CLI](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/table/sqlclient/).

## Usage

### As ScanTableSource

```sql
CREATE TEMPORARY TABLE heros (
  `name` STRING,
  `power` STRING, 
  `age` INT
) WITH (
  'connector' = 'faker', 
  'fields.name.expression' = '#{superhero.name}',
  'fields.power.expression' = '#{superhero.power}',
  'fields.power.null-rate' = '0.05',
  'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'
);
```
```sql
SELECT * FROM heros;
```


### As LookupTableSource

```sql
CREATE TEMPORARY TABLE location_updates (
  `character_id` INT,
  `location` STRING,
  `proctime` AS PROCTIME()
)
WITH (
  'connector' = 'faker', 
  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',
  'fields.location.expression' = '#{harry_potter.location}'
);
```
```sql
CREATE TEMPORARY TABLE characters (
  `character_id` INT,
  `name` STRING
)
WITH (
  'connector' = 'faker', 
  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',
  'fields.name.expression' = '#{harry_potter.characters}'
);
```
```sql
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
* `DATE`
* `TIME`
* `ARRAY`
* `MAP`
* `MULTISET`
* `ROW`

### Connector Options

| Connector Option            | Default | Description                                                                                                                      |
|-----------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------|
| `number-of-rows`            | None    | The number of rows to produce. If this is options is set, the source is bounded otherwise it is unbounded and runs indefinitely. |
| `rows-per-second`           | 10000   | The maximum rate at which the source produces records.                                                                           |
| `fields.<field>.expression` | None    | The [Data Faker](https://www.datafaker.net/documentation/expressions/) expression to generate the values for this field.         |
| `fields.<field>.null-rate`  | 0.0     | Fraction of rows for which this field is `null`                                                                                  |
| `fields.<field>.length`     | 1       | Size of array, map or multiset                                                                                                   |

### On Timestamps

For rows of type `TIMESTAMP`, `DATE` the corresponding Data Faker expression needs to return a timestamp formatted as `uuuu-MM-dd hh:mi:ss[.nnnnnnnnn]`.
Typically, you would use one of the following expressions:

```sql
CREATE TEMPORARY TABLE timestamp_time_and_date_example (
  `timestamp1` TIMESTAMP(3),
  `timestamp2` TIMESTAMP(3),
  `timestamp3` TIMESTAMP(3),
  `time`       TIME,
  `date1`      DATE,
  `date2`      DATE
)
WITH (
  'connector' = 'faker', 
  'fields.timestamp1.expression' = '#{date.past ''15'',''SECONDS''}',
  'fields.timestamp2.expression' = '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.timestamp3.expression' = '#{date.future ''15'',''5'',''SECONDS''}',
  'fields.time.expression' = '#{time.future ''15'',''5'',''SECONDS''}',
  'fields.date1.expression' = '#{date.birthday}',
  'fields.date2.expression' = '#{date.birthday ''1'',''100''}'
);
```
```sql
SELECT * FROM timestamp_time_and_date_example;
```

For `timestamp1` Data Faker will generate a random timestamp that lies at most 15 seconds in the past.
For `timestamp2` Data Faker will generate a random timestamp, that lies at most 15 seconds in the past, but at least 5 seconds.
For `timestamp3` Data Faker will generate a random timestamp, that lies at most 15 seconds in the future, but at least 5 seconds.
For `time` Data Faker will generate a random time, that lies at most 15 seconds in the future, but at least 5 seconds.
For `date1` Data Faker will generate a random birthday between 18 and 65 years ago.
For `date2` Data Faker will generate a random birthday between 1 and 100 years ago.

### On Collection Data Types

The usage of `ARRAY`, `MULTISET`, `MAP` and `ROW` types is shown in the following example.

```sql
CREATE TEMPORARY TABLE hp (
  `character-with-age` MAP<STRING,INT>,
  `spells` MULTISET<STRING>,
  `locations` ARRAY<STRING>,
  `house-points` ROW<`house` STRING, `points` INT>
) WITH (
  'connector' = 'faker',
  'fields.character-with-age.key.expression' = '#{harry_potter.character}',
  'fields.character-with-age.value.expression' = '#{number.numberBetween ''10'',''100''}',
  'fields.character-with-age.length' = '2',
  'fields.spells.expression' = '#{harry_potter.spell}',
  'fields.spells.length' = '5',
  'fields.locations.expression' = '#{harry_potter.location}',
  'fields.locations.length' = '3',
  'fields.house-points.house.expression' = '#{harry_potter.house}',
  'fields.house-points.points.expression' = '#{number.numberBetween ''10'',''100''}'
);
```
```sql
SELECT * FROM hp;
```

### "One Of" Columns

Datafaker allows to pick a random value from a list of options via expression ``Options.option``

```sql
CREATE TEMPORARY TABLE orders (
  `order_id` INT,
  `order_status` STRING
)
WITH (
  'connector' = 'faker',
  'fields.order_id.expression' = '#{number.numberBetween ''0'',''100''}',
  'fields.order_status.expression' = '#{Options.option ''RECEIVED'',''SHIPPED'',''CANCELLED'')}'
);
```
```sql
SELECT * FROM orders;
```

## License

Copyright © 2020-2022 Konstantin Knauf

Distributed under Apache License, Version 2.0.
