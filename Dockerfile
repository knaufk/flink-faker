FROM registry.ververica.com/v2.3/vvp-gateway:2.3.1
COPY connector-meta.yaml /vvp/sql/opt/connectors/faker/connector-meta.yaml
COPY target/flink-faker-0.1.0.jar /vvp/sql/opt/connectors/faker/flink-faker-0.1.0.jar
