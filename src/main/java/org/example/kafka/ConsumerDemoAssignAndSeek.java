package org.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ConsumerDemoAssignAndSeek {

    private static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static String GROUP_ID = "my-seven-application";
    private static String TOPIC = "first_topic";

    public static void main(String... args) {

        ExecutorService threadPool = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerThread consumerThread = new ConsumerThread(latch);
        threadPool.submit(consumerThread);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException ex) {
                log.info("app was interrupted", ex);
            }
            log.info("app was gracefully stopped");

        }));


    }

    private static class ConsumerThread implements Runnable {

        private final KafkaConsumer<String, String> consumer;
        private final CountDownLatch latch;

        private ConsumerThread(CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.consumer = new KafkaConsumer<>(properties);

            // assign and seek are mostly used to replay data or fetch a specific message

            // assign
            TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
            long offsetToReadFrom = 15;
            consumer.assign(Collections.singleton(partitionToReadFrom));

            // seek
            consumer.seek(partitionToReadFrom, offsetToReadFrom);

        }

        @Override
        public void run() {
//            consumer.subscribe(List.of(TOPIC));

            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // can throw WakeupException
                    records.forEach(r -> {
                        log.info("Key: {}, Value: {}", r.key(), r.value());
                        log.info("Partition: {}, Offset: {}", r.partition(), r.offset());
                    });
                } catch (WakeupException ex) {
                    consumer.close();
                    log.info("Consumer was closed");
                    latch.countDown();
                    break;
                }
            }
        }

        public void shutdown() {
            log.info("shut down consumer");
            consumer.wakeup();
        }
    }
}
