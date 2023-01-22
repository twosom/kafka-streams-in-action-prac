package com.icloud.app;

import com.icloud.custom.MyDeserializer;
import com.icloud.custom.MySerializer;
import com.icloud.model.StockTickerData;
import com.icloud.utils.MockDataProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.icloud.utils.MockDataProducer.STOCK_TICKER_STREAM_TOPIC;
import static com.icloud.utils.MockDataProducer.STOCK_TICKER_TABLE_TOPIC;

public class KStreamVSKTableExample {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamVSKTableExample.class);

    public static void main(String[] args) throws InterruptedException {
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // TODO table
        KTable<String, StockTickerData> stockTickerTable =
                streamsBuilder.table(STOCK_TICKER_TABLE_TOPIC, Materialized.as("stock-table-store"));

        // TODO stream
        KStream<String, StockTickerData> stockTickerStream =
                streamsBuilder.stream(STOCK_TICKER_STREAM_TOPIC);

        stockTickerTable.toStream()
                .print(Printed.<String, StockTickerData>toSysOut().withLabel(Thread.currentThread().getName() + " Stocks-KTable"));

        stockTickerStream.print(Printed.<String, StockTickerData>toSysOut().withLabel(Thread.currentThread().getName() + " Stocks-KStream"));

        int numberCompanies = 3;
        int iterations = 3;
        MockDataProducer.produceStockTickerData(numberCompanies, iterations);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        LOG.info("KTable vs KStream output started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(60_000);
        LOG.info("Shutting down KTable vs KStream Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStreamVSKTable_app");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KStreamVSKTable_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KStreamVSKTable_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "15000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StockTickerSerde().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }

    public static Serde<StockTickerData> StockTickerSerde() {
        return new StockTickerSerde();
    }

    public static class StockTickerSerde extends Serdes.WrapperSerde<StockTickerData> {
        public StockTickerSerde() {
            super(new MySerializer<>(), new MyDeserializer<>(StockTickerData.class));
        }
    }
}
