package com.icloud.app;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

import static com.icloud.utils.KafkaStreamPracUtils.stringSerde;

public class YellingApp {


    public static void main(String[] args) throws InterruptedException {
        var props = getConfig();
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("src-topic", Consumed.with(stringSerde, stringSerde))
                .mapValues(value -> value.toUpperCase())
                .to("out-topic", Produced.with(stringSerde, stringSerde));
        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
        Thread.sleep(35_000);
        kafkaStreams.close();
    }

    private static Properties getConfig() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }
}
