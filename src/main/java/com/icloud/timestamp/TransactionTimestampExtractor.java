package com.icloud.timestamp;

import com.icloud.model.Purchase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class TransactionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (!(record.value() instanceof Purchase purchase)) {
            throw new RuntimeException("invalid request");
        }
        return purchase.getPurchaseDate().getTime();
    }
}
