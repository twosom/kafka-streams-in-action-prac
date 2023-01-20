package com.icloud.app;

import com.icloud.custom.MyDeserializer;
import com.icloud.custom.MySerializer;
import com.icloud.model.Purchase;
import com.icloud.model.PurchasePattern;
import com.icloud.model.RewardAccumulator;
import com.icloud.process.supplier.PurchaseRewardsProcessorSupplier;
import com.icloud.utils.MockDataProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

import static com.icloud.utils.KafkaStreamPracUtils.stringSerde;

public class ZMartAppWithState {
    public static void main(String[] args) throws InterruptedException {
        Properties props = getProperties();
        MySerializer<Purchase> purchaseSerializer = new MySerializer<>();
        MyDeserializer<Purchase> purchaseDeserializer = new MyDeserializer<>(Purchase.class);
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseSerializer, purchaseDeserializer);

        MySerializer<PurchasePattern> purchasePatternSerializer = new MySerializer<>();
        MyDeserializer<PurchasePattern> purchasePatternDeserializer = new MyDeserializer<>(PurchasePattern.class);
        Serde<PurchasePattern> purchasePatternSerde = Serdes.serdeFrom(purchasePatternSerializer, purchasePatternDeserializer);

        MySerializer<RewardAccumulator> rewardAccumulatorSerializer = new MySerializer<>();
        MyDeserializer<RewardAccumulator> rewardAccumulatorDeserializer = new MyDeserializer<>(RewardAccumulator.class);
        Serde<RewardAccumulator> rewardAccumulatorSerde = Serdes.serdeFrom(rewardAccumulatorSerializer, rewardAccumulatorDeserializer);


        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // TODO source -> masking
        KStream<String, Purchase> maskedPurchasesKStream =
                streamsBuilder.stream("transactions", Consumed.with(stringSerde, purchaseSerde))
                        .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        // TODO masking -> sink
        maskedPurchasesKStream
                .to("purchases", Produced.with(stringSerde, purchaseSerde));


        StreamPartitioner<String, Purchase> streamPartitioner =
                (topic, key, value, numPartitions) -> value.getCustomerId().hashCode() % numPartitions;

        // TODO masking -> repartition
//        KStream<String, Purchase> transByCustomerStream = maskedPurchasesKStream
//                .repartition(Repartitioned.with(stringSerde, purchaseSerde)
//                        .withStreamPartitioner(streamPartitioner));
        //                .to("purchases", Produced.with(stringSerde, purchaseSerde))

        // TODO masking -> stateful rewards -> sink
        maskedPurchasesKStream
                .processValues(new PurchaseRewardsProcessorSupplier(),
                        "rewardsPointsStore")
                .peek((key, value) -> System.out.println(value.getCustomerId() + " :: " + value.getTotalRewardPoints()))
                .to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        // TODO masking -> pattern -> sink
        maskedPurchasesKStream
                .mapValues(purchase -> PurchasePattern.builder(purchase).build())
                .to("patterns", Produced.with(stringSerde, purchasePatternSerde));


        MockDataProducer.producePurchaseData();
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        Thread.sleep(60_000);
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "AddingStateConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "AddingStateGroupId");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AddingStateAppId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
