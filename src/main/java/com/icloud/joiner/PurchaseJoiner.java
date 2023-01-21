package com.icloud.joiner;

import com.icloud.model.CorrelatedPurchase;
import com.icloud.model.Purchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Date;

public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {

    @Override
    public CorrelatedPurchase apply(Purchase purchase, Purchase otherPurchase) {
        CorrelatedPurchase.Builder builder = CorrelatedPurchase.newBuilder();

        Date purchasedDate = getPurchasedDate(purchase);
        Double price = getPrice(purchase);
        String itemPurchased = getItemPurchased(purchase);

        Date otherPurchaseDate = getPurchasedDate(otherPurchase);
        Double otherPrice = getPrice(otherPurchase);
        String otherItemPurchased = getItemPurchased(otherPurchase);

        ArrayList<String> purchasedItem = new ArrayList<>();

        if (itemPurchased != null) {
            purchasedItem.add(itemPurchased);
        }

        if (otherItemPurchased != null) {
            purchasedItem.add(otherItemPurchased);
        }

        String customerId = getCustomerId(purchase);
        String otherCustomerId = getCustomerId(otherPurchase);

        builder.withCustomerId(customerId != null ? customerId : otherCustomerId)
                .withFirstPurchaseDate(purchasedDate)
                .withSecondPurchaseDate(otherPurchaseDate)
                .withItemsPurchased(purchasedItem)
                .withTotalAmount(price + otherPrice);

        return builder.build();
    }

    private String getCustomerId(Purchase purchase) {
        return purchase != null ? purchase.getCustomerId() : null;
    }

    private String getItemPurchased(Purchase purchase) {
        return purchase != null ? purchase.getItemPurchased() : null;
    }

    private double getPrice(Purchase purchase) {
        return purchase != null ? purchase.getPrice() : 0.0;
    }

    private Date getPurchasedDate(Purchase purchase) {
        return purchase != null ? purchase.getPurchaseDate() : null;
    }
}
