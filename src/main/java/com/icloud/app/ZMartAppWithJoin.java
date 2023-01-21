package com.icloud.app;

import com.icloud.custom.MyDeserializer;
import com.icloud.custom.MySerializer;
import com.icloud.joiner.PurchaseJoiner;
import com.icloud.model.CorrelatedPurchase;
import com.icloud.model.Purchase;
import com.icloud.model.PurchasePattern;
import com.icloud.model.RewardAccumulator;
import com.icloud.timestamp.TransactionTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

import static com.icloud.utils.KafkaStreamPracUtils.stringSerde;

public class ZMartAppWithJoin {

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

        MySerializer<CorrelatedPurchase> correlatedPurchaseSerializer = new MySerializer<>();
        MyDeserializer<CorrelatedPurchase> correlatedPurchaseDeserializer = new MyDeserializer<>(CorrelatedPurchase.class);
        Serde<CorrelatedPurchase> correlatedPurchaseSerde = Serdes.serdeFrom(correlatedPurchaseSerializer, correlatedPurchaseDeserializer);


        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // TODO source -> masking
        KStream<String, Purchase> maskedPurchaseKStream =
                streamsBuilder.stream("transactions", Consumed.with(stringSerde, purchaseSerde))
                        .mapValues(purchase -> Purchase.builder(purchase).maskCreditCard().build());


        // TODO masking -> selectKey ->filtering -> sink
        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey =
                (key, purchase) -> purchase.getPurchaseDate().getTime();
        Predicate<Long, Purchase> filterByPrice = (key, purchase) -> purchase.getPrice() > 5.00;
        maskedPurchaseKStream
                .selectKey(purchaseDateAsKey)
                .filter(filterByPrice)
                .peek((key, value) -> System.out.println("[selectedKey->filter] " + key + "::" + value.toString()))
                .to("purchases", Produced.with(Serdes.Long(), purchaseSerde));


        // TODO masking -> rewards -> sink
        maskedPurchaseKStream
                .mapValues(purchase -> RewardAccumulator.builder(purchase).build())
                .to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        // TODO masking -> pattern -> sink
        maskedPurchaseKStream
                .mapValues(purchase -> PurchasePattern.builder(purchase).build())
                .to("patterns", Produced.with(stringSerde, purchasePatternSerde));

        // TODO masking -> selectKey -> branch      -> join -> sink
        //                           -> electronics  /

        var purchaseJoiner = new PurchaseJoiner();
        JoinWindows twentyMinutesWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(20));

        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, value) -> value.getDepartment().equalsIgnoreCase("electronics");
        KeyValueMapper<String, Purchase, String> customerIdAsKey = (key, purchase) -> purchase.getCustomerId();
        var branches = maskedPurchaseKStream
                .selectKey(customerIdAsKey)
                .peek((key, value) -> System.out.println("[customerIdKey]" + key))
                .split(Named.as("stream-"))
                .branch(isCoffee, Branched.as("coffee"))
                .branch(isElectronics, Branched.as("electronics"))
                .noDefaultBranch();

        KStream<String, Purchase> coffeeStream = branches.get("stream-coffee");
        KStream<String, Purchase> electronicsStream = branches.get("stream-electronics");
        KStream<String, CorrelatedPurchase> joinedKStream = coffeeStream.join(electronicsStream,
                purchaseJoiner,
                twentyMinutesWindow,
                StreamJoined.with(stringSerde, purchaseSerde, purchaseSerde));

        joinedKStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("joinedStream"));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        Thread.sleep(60_000);
        kafkaStreams.close();


    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_application");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
        return props;
    }
}
