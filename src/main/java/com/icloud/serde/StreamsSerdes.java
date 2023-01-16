package com.icloud.serde;

import com.icloud.model.Purchase;
import com.icloud.model.PurchasePattern;
import com.icloud.model.RewardAccumulator;
import com.icloud.serializer.__JsonDeserializer;
import com.icloud.serializer.__JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;


public class StreamsSerdes {

    public static Serde<PurchasePattern> PurchasePatternSerde() {
        return new PurchasePatternSerde();
    }

    public static Serde<Purchase> PurchaseSerde() {
        return new PurchaseSerde();
    }

    public static Serde<RewardAccumulator> RewardAccumulatorSerde() {
        return new RewardAccumulatorSerde();
    }

    public static final class PurchaseSerde extends WrapperSerde<Purchase> {
        public PurchaseSerde() {
            super(new __JsonSerializer<>(), new __JsonDeserializer<>());
        }
    }

    public static final class PurchasePatternSerde extends WrapperSerde<PurchasePattern> {
        public PurchasePatternSerde() {
            super(new __JsonSerializer<>(), new __JsonDeserializer<>());
        }
    }

    public static final class RewardAccumulatorSerde extends WrapperSerde<RewardAccumulator> {
        public RewardAccumulatorSerde() {
            super(new __JsonSerializer<>(), new __JsonDeserializer<>());
        }
    }


}
