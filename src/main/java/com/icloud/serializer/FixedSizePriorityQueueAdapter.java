package com.icloud.serializer;


import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.icloud.collectors.FixedSizePriorityQueue;
import com.icloud.model.ShareVolume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class FixedSizePriorityQueueAdapter extends TypeAdapter<FixedSizePriorityQueue<ShareVolume>> {

    private Gson gson = new Gson();

    @Override
    public void write(JsonWriter writer, FixedSizePriorityQueue<ShareVolume> value) throws IOException {

        if (value == null) {
            writer.nullValue();
            return;
        }


        Iterator<ShareVolume> iterator = value.iterator();
        List<ShareVolume> list = new ArrayList<>();
        while (iterator.hasNext()) {
            ShareVolume stockTransaction = iterator.next();
            if (stockTransaction != null) {
                list.add(stockTransaction);
            }
        }
        writer.beginArray();
        for (ShareVolume transaction : list) {
            writer.value(gson.toJson(transaction));
        }
        writer.endArray();
    }

    @Override
    public FixedSizePriorityQueue<ShareVolume> read(JsonReader reader) throws IOException {
        Comparator<ShareVolume> c = (c1, c2) -> c2.getShares() - c1.getShares();
        FixedSizePriorityQueue<ShareVolume> fixedSizePriorityQueue = new FixedSizePriorityQueue<>(c, 5);
        JsonValueHolder holder = gson.fromJson(reader, JsonValueHolder.class);
        holder.inner.forEach(fixedSizePriorityQueue::add);
        return fixedSizePriorityQueue;
    }

    static class JsonValueHolder {
        Integer maxSize;

        List<ShareVolume> inner;
    }

}
