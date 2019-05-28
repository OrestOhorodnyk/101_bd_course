package consumer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import consumer.BookingEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class App {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .set("es.index.auto.create", "true")
                .set("es.nodes", "sandbox-hdp.hortonworks.com")
                .set("es.port", "9200")
                .setAppName("Read from kafka topic");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Setup a streaming context.
        JavaSparkContext ctx = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
        JavaStreamingContext streamingContext = new JavaStreamingContext(ctx, Durations.seconds(3));

        // Create a map of Kafka params
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        // List of Kafka brokers to listen to.
        kafkaParams.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "grop_id_1");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        // List of topics to listen to.
        Collection<String> topics = Arrays.asList("101_streaming_hw");

        // Create a Spark DStream with the kafka topics.
        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        // Setup a processing map function that only returns the payload.
        JavaDStream<BookingEvent> retval = stream.map(new Function<ConsumerRecord<String, String>, BookingEvent>() {

            private static final long serialVersionUID = 1L;

            public BookingEvent call(ConsumerRecord<String, String> record) {
                try {

                    // Convert the payload to a Json node and then extract relevant data.
                    String jsonString = record.value();
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode docRoot = objectMapper.readTree(jsonString);
                    return new BookingEvent(docRoot);

                } catch (Exception e) {
                    e.printStackTrace();
                }

                return new BookingEvent();
            }
        });

        retval.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                Dataset<Row> ds = spark.createDataFrame(rdd.rdd(), BookingEvent.class);
                ds.show();
                JavaEsSpark.saveToEs(rdd, "spark/log");
            }
        });

        // Start streaming.
        streamingContext.start();

        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        streamingContext.close();

    }
}