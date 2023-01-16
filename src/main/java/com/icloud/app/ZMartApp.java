package com.icloud.app;

import com.icloud.custom.MyDeserializer;
import com.icloud.custom.MySerializer;
import com.icloud.model.Purchase;
import com.icloud.model.PurchasePattern;
import com.icloud.model.RewardAccumulator;
import com.icloud.service.SecurityDBService;
import com.icloud.utils.MockDataProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

import static com.icloud.utils.KafkaStreamPracUtils.stringSerde;

public class ZMartApp {
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

        Predicate<String, Purchase> isCoffee = (key, purchase) ->
                purchase.getDepartment().equalsIgnoreCase("coffee");

        Predicate<String, Purchase> isElectronics = (key, purchase) ->
                purchase.getDepartment().equalsIgnoreCase("electronics");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // TODO source -> masking
        KStream<String, Purchase> purchaseKStream = streamsBuilder
                .stream("transactions", Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        // TODO masking -> branch -> coffee
        //                        -> electronics
        purchaseKStream.split()
                .branch(isCoffee, Branched.withConsumer(stream -> stream
                        .peek((key, value) -> System.out.println(":::COFFEE:::"))
                        .to("coffee", Produced.with(stringSerde, purchaseSerde))))
                .branch(isElectronics, Branched.withConsumer(stream -> stream
                        .peek((key, value) -> System.out.println(":::ELECTRONICS:::"))
                        .to("electronics", Produced.with(stringSerde, purchaseSerde))));


        // TODO masking -> pattern -> sink
        purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build())
                .to("patterns", Produced.with(stringSerde, purchasePatternSerde));

        // TODO masking -> rewords -> sink
        purchaseKStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build())
                .to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        // TODO masking -> filter -> sink
        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase.getPurchaseDate().getTime();
        purchaseKStream.filter((key, purchase) -> purchase.getPrice() > 5.00)
                .selectKey(purchaseDateAsKey)
                .peek((key, value) -> System.out.println("key::" + key + "\nvalue::" + value))
                .to("purchases", Produced.with(Serdes.Long(), purchaseSerde));

        // TODO masking -> filter -> foreach
        ForeachAction<String, Purchase> purchaseForeachAction = (key, purchase) ->
                SecurityDBService.saveRecord(purchase.getPurchaseDate(), purchase.getEmployeeId(), purchase.getItemPurchased());
        Predicate<String, Purchase> filterSpecifyEmployeeId = (key, purchase) ->
                purchase.getEmployeeId().equalsIgnoreCase("48971");
        purchaseKStream.filter(filterSpecifyEmployeeId)
                .foreach(purchaseForeachAction);


        MockDataProducer.producePurchaseData();
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        Thread.sleep(60_000);
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }


}
