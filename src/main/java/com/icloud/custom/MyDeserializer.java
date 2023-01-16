package com.icloud.custom;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

public class MyDeserializer<T> implements Deserializer<T> {

    private final Gson gson = new Gson();
    private Class<T> deserializedClass;

    public MyDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        return gson.fromJson(new String(data), deserializedClass);
    }


}
