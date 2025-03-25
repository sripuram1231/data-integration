package kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaMessageLogger {
    private static final Logger logger = LogManager.getLogger(KafkaMessageLogger.class);
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        Properties appProps = loadProperties();
        if (appProps == null) {
            return;
        }

        // Configure and initialize Kafka producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appProps.getProperty("kafka.bootstrap.servers"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        // Configure Kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appProps.getProperty("kafka.bootstrap.servers"));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, appProps.getProperty("kafka.consumer.group.id.logger"));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, appProps.getProperty("kafka.consumer.auto.offset.reset"));

// Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

// Subscribe to the topic
        String inputTopic = appProps.getProperty("kafka.topic.input");
        consumer.subscribe(Collections.singletonList(inputTopic));
        String bootStrapServer = appProps.getProperty("kafka.bootstrap.servers");
        logger.info("bootStrapServer " + bootStrapServer);
        logger.info("Started listening to " + inputTopic + " topic...");

        try {
            logger.info("Consumer started with bootstrap server: {}", bootStrapServer);

            // Add a counter to track message processing
            int messageCount = 0;

            while (true) {
                // Increase polling time to give more time to fetch messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.count() > 0) {
                    logger.info("Received {} records", records.count());
                } else {
                    // Only log occasionally to avoid flooding logs
                    if (messageCount % 10 == 0) {
                        logger.info("No records received in this poll");
                    }
                }

                for (ConsumerRecord<String, String> record : records) {
                    messageCount++;
                    logger.info("Received message #{}: Topic = {}, Partition = {}, Offset = {}, Key = {}, Value = {}",
                            messageCount,
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());

                    // Forward message to user listener topic
                    String userListenerTopic = appProps.getProperty("kafka.topic.user.listener");
                    ProducerRecord<String, String> producerRecord = 
                        new ProducerRecord<>(userListenerTopic, record.key(), record.value());
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception == null) {
                            logger.info("Message forwarded to topic {} at partition {} with offset {}",
                                    metadata.topic(), metadata.partition(), metadata.offset());
                        } else {
                            logger.error("Error sending message to topic " + userListenerTopic, exception);
                        }
                    });
                }

                // Commit offsets if auto-commit is disabled
                if (!Boolean.parseBoolean(consumerProps.getProperty("enable.auto.commit", "true"))) {
                    consumer.commitSync();
                    logger.info("Committed offsets synchronously");
                }

                // Optional: Add a small delay to prevent CPU spinning in case of no messages
                if (records.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        logger.error("Consumer thread interrupted", e);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error consuming messages", e);
        } finally {
            // Close the consumer
            consumer.close();
            producer.close();
            logger.info("Consumer and producer closed");
        }

        // Kafka producer added and set up to forward messages to WFS-USER-LISTENER topic

    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = KafkaMessageLogger.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                logger.error("Unable to find application.properties");
                return null;
            }
            properties.load(input);
            return properties;
        } catch (IOException ex) {
            logger.error("Error loading properties file: " + ex.getMessage());
            return null;
        }
    }
}

