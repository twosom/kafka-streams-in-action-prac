package com.icloud.custom;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.icloud.collectors.FixedSizePriorityQueue;
import com.icloud.serializer.FixedSizePriorityQueueAdapter;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.util.Map;

public class MyDeserializer<T> implements Deserializer<T> {

    private Gson gson;
    private Class<T> deserializedClass;
    private Type reflectionTypeToken;


    public MyDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
        init();
    }

    public MyDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
        init();
    }

    public MyDeserializer() {
    }

    private void init() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(FixedSizePriorityQueue.class, new FixedSizePriorityQueueAdapter().nullSafe());
        gson = builder.create();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> map, boolean b) {
        if (deserializedClass == null) {
            deserializedClass = (Class<T>) map.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        Type deserializeFrom = deserializedClass != null ? deserializedClass : reflectionTypeToken;
        return gson.fromJson(new String(bytes), deserializeFrom);
    }

    @Override
    public void close() {

    }


}
