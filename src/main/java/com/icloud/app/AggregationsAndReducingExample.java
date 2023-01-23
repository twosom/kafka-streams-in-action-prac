package com.icloud.app;

import com.icloud.collectors.FixedSizePriorityQueue;
import com.icloud.custom.MyDeserializer;
import com.icloud.custom.MySerializer;
import com.icloud.model.ShareVolume;
import com.icloud.model.StockTransaction;
import com.icloud.utils.KafkaStreamPracUtils;
import com.icloud.utils.MockDataProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

import static com.icloud.utils.MockDataProducer.STOCK_TRANSACTIONS_TOPIC;

public class AggregationsAndReducingExample {

    private static Logger LOG = LoggerFactory.getLogger(AggregationsAndReducingExample.class);

    public static Serde<FixedSizePriorityQueue> FixedSizePriorityQueueSerde() {
        return new FixedSizePriorityQueueSerde();
    }

    public static final class FixedSizePriorityQueueSerde extends Serdes.WrapperSerde<FixedSizePriorityQueue> {
        public FixedSizePriorityQueueSerde() {
            super(new MySerializer<>(), new MyDeserializer<>(FixedSizePriorityQueue.class));
        }
    }

    public static void main(String[] args) throws InterruptedException {
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        MySerializer<StockTransaction> stockTransactionSerializer = new MySerializer<>();
        MyDeserializer<StockTransaction> stockTransactionDeserializer = new MyDeserializer<>(StockTransaction.class);
        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionSerializer, stockTransactionDeserializer);
        Serde<String> stringSerde = Serdes.String();
        MySerializer<ShareVolume> shareVolumeSerializer = new MySerializer<>();
        MyDeserializer<ShareVolume> shareVolumeDeserializer = new MyDeserializer<>(ShareVolume.class);
        Serde<ShareVolume> shareVolumeSerde = Serdes.serdeFrom(shareVolumeSerializer, shareVolumeDeserializer);
        Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = FixedSizePriorityQueueSerde();

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // TODO StockTransaction(거래 데이터) -> ShareVolume(거래량) -> Grouping -> Reduce
        KTable<String, ShareVolume> shareVolumeKTable = streamsBuilder.stream(STOCK_TRANSACTIONS_TOPIC,
                        Consumed.with(stringSerde, stockTransactionSerde)
                                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .mapValues(stockTransaction -> ShareVolume.newBuilder(stockTransaction).build())
                // TODO groupBy 를 사용함으로써 key 가 바뀐다. -> repartition 발생 의미
                .groupBy((k, v) -> v.getSymbol(), Grouped.with(stringSerde, shareVolumeSerde))
                .reduce(ShareVolume::sum);

        Comparator<ShareVolume> comparator = (sv1, sv2) -> sv2.getShares() - sv1.getShares();
        FixedSizePriorityQueue<ShareVolume> fixedQueue = new FixedSizePriorityQueue<>(comparator, 5);

        NumberFormat numberFormat = NumberFormat.getInstance();
        ValueMapper<FixedSizePriorityQueue, String> valueMapper = fpq -> {
            StringBuilder builder = new StringBuilder();
            Iterator<ShareVolume> iterator = fpq.iterator();
            int counter = 1;
            while (iterator.hasNext()) {
                ShareVolume stockVolume = iterator.next();
                if (stockVolume != null) {
                    builder.append("\n").append(counter++);
                    builder.append(")");
                    builder.append(stockVolume.getSymbol());
                    builder.append(":");
                    builder.append(numberFormat.format(stockVolume.getShares()));
                    builder.append("\n");
                }
            }
            return builder.toString();
        };

        shareVolumeKTable.groupBy((k, v) -> KeyValue.pair(v.getIndustry(), v), Grouped.with(stringSerde, shareVolumeSerde))
                .aggregate(() -> fixedQueue,
                        (k, v, agg) -> agg.add(v),  // TODO 레코드가 도착하면 해당 레코드 추가
                        (k, v, agg) -> agg.remove(v), // TODO 이미 같은 키의 레코드가 있다면 기존 레코드 제거
                        Materialized.with(stringSerde, fixedSizePriorityQueueSerde))
                .mapValues(valueMapper)
                .toStream().peek((key, value) -> LOG.info("Stock volume by industry {} {}", key, value))
                .to("stock-volume-by-company", Produced.with(stringSerde, KafkaStreamPracUtils.stringSerde));
        ;


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        MockDataProducer.produceStockTransactions(15, 50, 25, false);
        LOG.info("First Reduction and Aggregation Example Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Reduction and Aggregation Example Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KTable-aggregations");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KTable-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KTable-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 30_000);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10_000);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 10_000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }
}
