package com.icloud.utils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class KafkaStreamPracUtils {
    public static final Serde<String> stringSerde = Serdes.String();
}
