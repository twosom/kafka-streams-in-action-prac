package com.icloud.custom;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class MySerializer<T> implements Serializer<T> {

    private final Gson gson = new Gson();

    @Override
    public byte[] serialize(String topic, T t) {
        return gson.toJson(t).getBytes(StandardCharsets.UTF_8);
    }
}
