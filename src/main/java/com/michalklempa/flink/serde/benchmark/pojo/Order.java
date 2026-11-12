package com.michalklempa.flink.serde.benchmark.pojo;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfo;

public class Order {
    public String orderId;
    public long createdAtMillis;
    public long updatedAtMillis;
    public OrderStatus status;
    public Customer customer;

    @TypeInfo(LineItemListTypeInfoFactory.class)
    public List<LineItem> items;

    @TypeInfo(StringListTypeInfoFactory.class)
    public List<String> tags;

    @TypeInfo(StringMapTypeInfoFactory.class)
    public Map<String, String> attributes;

    public double totalAmount;
    public byte[] payload;

    public Order() {
    }
}
