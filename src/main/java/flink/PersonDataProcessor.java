package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
//import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.connector.base.DeliveryGuarantee;

//import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

public class PersonDataProcessor {

    public static void main(String[] args) throws Exception {

        try {
            // Set up the streaming execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Parse program arguments
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);

            // Configure Kafka consumer
            Properties consumerProps = new Properties();
            consumerProps.setProperty("bootstrap.servers", parameterTool.get("bootstrap.servers", "localhost:29092"));
            consumerProps.setProperty("group.id", "person-data-processor");
            consumerProps.setProperty("auto.offset.reset", "earliest");

            // Create Kafka consumer
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(consumerProps.getProperty("bootstrap.servers"))
                    .setGroupId(consumerProps.getProperty("group.id"))
                    .setTopics("WFS-USER-LISTENER")
                    .setStartingOffsets(OffsetsInitializer.earliest()) // or latest() depending on your needs
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            // Configure Kafka producer
            Properties producerProps = new Properties();
            producerProps.setProperty("bootstrap.servers", parameterTool.get("bootstrap.servers", "localhost:29092"));
            producerProps.setProperty("transaction.timeout.ms", "5000");

            // Create Kafka producer
            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(producerProps.getProperty("bootstrap.servers"))
                    .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                            .setTopic("WFS-USER-LISTENER")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    )
                    //.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // Or EXACTLY_ONCE if needed
                    .build();

            // Create data stream from Kafka
            //DataStream<String> stream = env.addSource(consumer);

            DataStream<String> stream = env.fromSource(
                    source,
                    WatermarkStrategy.noWatermarks(),
                    "Kafka Source");
            // Process the data
            DataStream<String> processedStream = stream.map(new PersonDataProcessor.PersonDataTransformer());

            // Write the processed data to Kafka
            processedStream.sinkTo(sink);

            // Print the processed data to the console
            processedStream.print();
            System.out.println("Person Data Processor started");

            // Execute the Flink job
            env.execute("Person Data Processor");
        } catch (Exception e) {
            System.err.println("Error executing Person Data Processor: " + e.getMessage());
            e.printStackTrace();
            System.exit(1); // This explicitly sets the exit code to 1
        }
    }

    /**
     * Custom transformation logic for person data
     */
    public static class PersonDataTransformer implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {
            // Here you can implement your specific processing logic
            // For example, parse JSON, transform data, enrich data, etc.

            // This is a simple example that adds a processing timestamp
            return String.format("{\"processed_at\": \"%s\", \"original_data\": %s}",
                    System.currentTimeMillis(),
                    value);
        }
    }
}

