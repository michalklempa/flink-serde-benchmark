package com.michalklempa.flink.serde.benchmark.pojoplain;

import java.util.List;
import java.util.Map;

public class Order {
    public String orderId;
    public long createdAtMillis;
    public long updatedAtMillis;
    public OrderStatus status;
    public Customer customer;
    public List<LineItem> items;
    public List<String> tags;
    public Map<String, String> attributes;
    public double totalAmount;
    public byte[] payload;

    public Order() {
    }
}
