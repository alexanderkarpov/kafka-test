package org.example.kafka;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Optional;
import java.util.Properties;

@Slf4j
public class ProducerDemoWithKeys {

    public static void main(String... args) {

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        // see: https://kafka.apache.org/documentation/#producerconfigs
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            // create a producer record
            String topic = "first_topic";
            String value = "hello world #" + i + " at " + new Date();
            String key = "id_" + i; // same key goes to the same partition
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // send data (async)
            producer.send(record, (metadata, e) ->
                    // executes every time a record is successfully sent or an exception is thrown
                    Optional.ofNullable(e)
                            .ifPresentOrElse(ex -> log.error("Error while producing", ex),
                                    () -> log.info("Received new metadata.\n" +
                                                    "Topic: {}\n" +
                                                    "Partition: {}\n" +
                                                    "Offset: {}\n" +
                                                    "Timestamp: {}",
                                            metadata.topic(), metadata.partition(), metadata.offset(),
                                            new Date(metadata.timestamp())))

            );
        }

        producer.flush();
        producer.close();

    }
}
