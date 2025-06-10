package org.example.backward;

import java.util.List;
import lombok.Data;
import org.apache.avro.reflect.Nullable;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BackwardCompatibilitySuccessTest {

    private static final Logger log = LoggerFactory.getLogger(BackwardCompatibilitySuccessTest.class);
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String ADMIN_URL = "http://localhost:8080";
    private static final String TENANT = "public";
    private static final String NAMESPACE = "schema-testing";
    private static final String NAMESPACE_FULL_NAME = TENANT + "/" + NAMESPACE;
    
    @Data
    static class MyRecordV1 {
        private String field1;
        private String field2;
    }

    @Data
    static class MyRecordV2 {
        private String field1;
        @Nullable
        private String field3;
    }

    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(ADMIN_URL)
                .build();
        String topic = "persistent://" + TENANT + "/" + NAMESPACE + "/schema-test" + System.currentTimeMillis();
        String subName = "backward-compatibility-sub";
        
        // 0. Define schemas for MyRecordV1 and MyRecordV3
        Schema<MyRecordV1> schemaV1 = Schema.AVRO(
                SchemaDefinition.<MyRecordV1>builder().withAlwaysAllowNull
                        (false).withPojo(MyRecordV1.class).build());
        Schema<MyRecordV2> schemaV2 = Schema.AVRO(
                SchemaDefinition.<MyRecordV2>builder().withAlwaysAllowNull
                        (false).withPojo(MyRecordV2.class).build());

        // 1. Create namespace and set schema compatibility strategy to BACKWARD
        createNamespaceWithBackwardCompatibility(admin);
        admin.topics().createSubscription(topic, subName, MessageId.earliest);

        // 2. Produce messages with V1 schema
        Producer<MyRecordV1> producerV1 = client.newProducer(schemaV1)
                .topic(topic)
                .create();
        MyRecordV1 recordV1 = new MyRecordV1();
        recordV1.setField1("V1-Field1");
        recordV1.setField2("V1-Field2");
        producerV1.send(recordV1);
        log.info("Send V1 schema message: {}", recordV1);
        List<SchemaInfo> allSchemas = admin.schemas().getAllSchemas(topic);
        log.info("After send v1 All schemas: {}", allSchemas);

        // 3. Produce messages with V2 schema (should succeed due to backward compatibility)
        Producer<MyRecordV2> producerV2 = client.newProducer(schemaV2)
                .topic(topic)
                .create();
        MyRecordV2 recordV2 = new MyRecordV2();
        recordV2.setField1("V2-Field1");
        recordV2.setField3("V2-new-field");
        producerV2.send(recordV2);
        log.info("Send V2 schema message: {}", recordV2);
        allSchemas = admin.schemas().getAllSchemas(topic);
        log.info("After send v2 All schemas: {}", allSchemas);

        // 5. Consume messages with V1 schema (should be able failed)
        Consumer<MyRecordV2> consumer = client.newConsumer(schemaV2)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
        for (int i = 0; i < 2; i++) {
            Message<MyRecordV2> message = consumer.receive();
            MyRecordV2 record = message.getValue();
            log.info("Receive message after V2 send: {}", record);
            consumer.acknowledge(message);
        }
        log.info("Consumer received all messages successfully");
        client.close();
        admin.close();
    }

    private static void createNamespaceWithBackwardCompatibility(PulsarAdmin admin) throws Exception {
        NamespaceName namespaceName = NamespaceName.get(NAMESPACE_FULL_NAME);
        if (!admin.namespaces().getNamespaces(TENANT).contains(namespaceName.toString())) {
            admin.namespaces().createNamespace(namespaceName.toString());
        }
        admin.namespaces().setSchemaCompatibilityStrategy(
                namespaceName.toString(),
                SchemaCompatibilityStrategy.BACKWARD
        );
        SchemaCompatibilityStrategy schemaCompatibilityStrategy =
                admin.namespaces().getSchemaCompatibilityStrategy(namespaceName.toString());
        log.info("Set namespace {} to {}", namespaceName, schemaCompatibilityStrategy);
    }
}    