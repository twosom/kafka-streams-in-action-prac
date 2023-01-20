package com.icloud.process.supplier;

import com.icloud.model.Purchase;
import com.icloud.model.RewardAccumulator;
import com.icloud.process.PurchaseRewardsProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Set;

public class PurchaseRewardsProcessorSupplier implements FixedKeyProcessorSupplier<String, Purchase, RewardAccumulator> {

    private final String rewardsStateStoreName = "rewardsPointsStore";

    @Override
    public FixedKeyProcessor<String, Purchase, RewardAccumulator> get() {
        return new PurchaseRewardsProcessor(rewardsStateStoreName);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());
        return Collections.singleton(storeBuilder);
    }
}
