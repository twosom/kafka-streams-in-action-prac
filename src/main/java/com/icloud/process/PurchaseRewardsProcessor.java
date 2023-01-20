package com.icloud.process;

import com.icloud.model.Purchase;
import com.icloud.model.RewardAccumulator;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

public class PurchaseRewardsProcessor implements FixedKeyProcessor<String, Purchase, RewardAccumulator> {

    private KeyValueStore<String, Integer> stateStore;
    private final String storeName;
    private FixedKeyProcessorContext<String, RewardAccumulator> context;

    public PurchaseRewardsProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, RewardAccumulator> context) {
        this.context = context;
        this.stateStore = this.context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, Purchase> record) {
        Purchase purchase = record.value();
        RewardAccumulator rewardAccumulator = RewardAccumulator.builder(purchase).build();
        Integer accumulatedSoFar = stateStore.get(rewardAccumulator.getCustomerId());

        if (accumulatedSoFar != null) {
            rewardAccumulator.addRewardPoints(accumulatedSoFar);
        }

        stateStore.put(rewardAccumulator.getCustomerId(), rewardAccumulator.getTotalRewardPoints());
        context.forward(record.withValue(rewardAccumulator));
    }
}
