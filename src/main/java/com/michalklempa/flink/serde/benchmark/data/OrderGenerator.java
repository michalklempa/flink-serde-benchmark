package com.michalklempa.flink.serde.benchmark.data;

import com.michalklempa.flink.serde.benchmark.data.OrderSpec.AddressSpec;
import com.michalklempa.flink.serde.benchmark.data.OrderSpec.CustomerSpec;
import com.michalklempa.flink.serde.benchmark.data.OrderSpec.LineItemSpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public final class OrderGenerator {
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static final int STATUS_COUNT = 5;

    private final Random random;
    private final Profile profile;
    private long sequence;

    public OrderGenerator(long seed, Profile profile) {
        this.random = new Random(seed);
        this.profile = profile;
    }

    public OrderSpec next() {
        long created = 1_700_000_000_000L + sequence * 1000 + random.nextInt(1000);
        String orderId = "ord-" + (sequence++) + "-" + text(8);
        CustomerSpec customer = new CustomerSpec(
                1 + random.nextInt(1_000_000),
                text(profile.stringLength),
                text(profile.stringLength) + "@example.com",
                random.nextBoolean(),
                new AddressSpec(text(profile.stringLength), text(profile.stringLength), text(2).toUpperCase(),
                        String.valueOf(10000 + random.nextInt(90000))));
        List<LineItemSpec> items = new ArrayList<>(profile.items);
        double total = 0;
        for (int i = 0; i < profile.items; i++) {
            int quantity = 1 + random.nextInt(10);
            double unitPrice = money();
            double discount = random.nextInt(4) * 5.0;
            total += quantity * unitPrice * (1 - discount / 100);
            items.add(new LineItemSpec("sku-" + text(profile.stringLength), text(profile.stringLength), quantity,
                    unitPrice, discount));
        }
        List<String> tags = new ArrayList<>(profile.tags);
        for (int i = 0; i < profile.tags; i++) {
            tags.add(text(profile.stringLength));
        }
        Map<String, String> attributes = new LinkedHashMap<>();
        for (int i = 0; i < profile.attributes; i++) {
            attributes.put("attr-" + i + "-" + text(4), text(profile.stringLength));
        }
        byte[] payload = new byte[profile.payloadBytes];
        random.nextBytes(payload);
        return new OrderSpec(orderId, created, created + random.nextInt(100_000), random.nextInt(STATUS_COUNT),
                customer, items, tags, attributes, Math.round(total * 100) / 100.0, payload);
    }

    private double money() {
        return (1 + random.nextInt(100_000)) / 100.0;
    }

    private String text(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }
}
