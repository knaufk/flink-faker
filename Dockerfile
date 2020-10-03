FROM eu.gcr.io/vvp-devel-240810/vvp-gateway:20201003193311-01d1bb5
COPY connector-meta.yaml /vvp/sql/opt/connectors/faker/connector-meta.yaml
COPY target/flink-faker-0.1-SNAPSHOT.jar /vvp/sql/opt/connectors/faker/flink-faker-0.1-SNAPSHOT.jar