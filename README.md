# pulsar-schema-test


Run pulsar standalone
```
docker run -it \
-p 6650:6650 \
-p 8080:8080 \
apachepulsar/pulsar:4.0.5 \
bin/pulsar standalone
```


Apache pulsar test: https://github.com/apache/pulsar/blob/82237d3684fe506bcb6426b3b23f413422e6e4fb/pulsar-broker/src/test/java/org/apache/pulsar/broker/service/schema/BaseAvroSchemaCompatibilityTest.java#L88-L119