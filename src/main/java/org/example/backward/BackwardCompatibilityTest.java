package org.example.backward;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.example.MyRecordV1;
import org.example.MyRecordV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BackwardCompatibilityTest {

    private static final Logger log = LoggerFactory.getLogger(BackwardCompatibilityTest.class);
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String ADMIN_URL = "http://localhost:8080";
    private static final String TENANT = "public";
    private static final String NAMESPACE = "schema-testing";
    private static final String NAMESPACE_FULL_NAME = TENANT + "/" + NAMESPACE;

    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(ADMIN_URL)
                .build();
        String topic = "persistent://" + TENANT + "/" + NAMESPACE + "/schema-test" + System.currentTimeMillis();
        String subName = "backward-compatibility-sub";

        // 1. Create namespace and set schema compatibility strategy to BACKWARD
        createNamespaceWithBackwardCompatibility(admin);
        admin.topics().createSubscription(topic, subName, MessageId.earliest);

        // 2. Define V1 and V2 schemas
        AvroSchema<MyRecordV1> schemaV1 = AvroSchema.of(MyRecordV1.class);
        AvroSchema<MyRecordV2> schemaV2 = AvroSchema.of(MyRecordV2.class);

        // 3. Produce messages with V1 schema
        Producer<MyRecordV1> producerV1 = client.newProducer(schemaV1)
                .topic(topic)
                .create();
        MyRecordV1 recordV1 = new MyRecordV1();
        recordV1.setField1("V1-Field1");
        recordV1.setField2("V1-Field2");
        producerV1.send(recordV1);
        log.info("Send V1 schema message: {}", recordV1);

        // 4. Produce messages with V2 schema (should succeed due to backward compatibility)
        Producer<MyRecordV2> producerV2 = client.newProducer(schemaV2)
                .topic(topic)
                .create();
        MyRecordV2 recordV2 = new MyRecordV2();
        recordV2.setField1("V2-Field1");
        recordV2.setField2("V2-Field2");
        recordV2.setField3("V2-new-field");
        producerV2.send(recordV2);
        log.info("Send V2 schema message: {}", recordV2);

        // 5. Consume messages with V1 schema (should be able to consume both V1 and V2 messages)
        Consumer<MyRecordV1> consumer = client.newConsumer(schemaV1)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
        for (int i = 0; i < 2; i++) {
            Message<MyRecordV1> message = consumer.receive();
            MyRecordV1 record = message.getValue();
            log.info("Receive message: {}", record);
            consumer.acknowledge(message);
        }
        client.close();
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
        log.info("Set namespace {} to BACKWARD", namespaceName);
    }
}    