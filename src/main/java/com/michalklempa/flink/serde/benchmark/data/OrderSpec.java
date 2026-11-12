package com.michalklempa.flink.serde.benchmark.data;

import java.util.List;
import java.util.Map;

public final class OrderSpec {
    public final String orderId;
    public final long createdAtMillis;
    public final long updatedAtMillis;
    public final int status;
    public final CustomerSpec customer;
    public final List<LineItemSpec> items;
    public final List<String> tags;
    public final Map<String, String> attributes;
    public final double totalAmount;
    public final byte[] payload;

    public OrderSpec(String orderId, long createdAtMillis, long updatedAtMillis, int status, CustomerSpec customer,
                     List<LineItemSpec> items, List<String> tags, Map<String, String> attributes,
                     double totalAmount, byte[] payload) {
        this.orderId = orderId;
        this.createdAtMillis = createdAtMillis;
        this.updatedAtMillis = updatedAtMillis;
        this.status = status;
        this.customer = customer;
        this.items = items;
        this.tags = tags;
        this.attributes = attributes;
        this.totalAmount = totalAmount;
        this.payload = payload;
    }

    public static final class CustomerSpec {
        public final long customerId;
        public final String name;
        public final String email;
        public final boolean vip;
        public final AddressSpec address;

        public CustomerSpec(long customerId, String name, String email, boolean vip, AddressSpec address) {
            this.customerId = customerId;
            this.name = name;
            this.email = email;
            this.vip = vip;
            this.address = address;
        }
    }

    public static final class AddressSpec {
        public final String street;
        public final String city;
        public final String country;
        public final String postalCode;

        public AddressSpec(String street, String city, String country, String postalCode) {
            this.street = street;
            this.city = city;
            this.country = country;
            this.postalCode = postalCode;
        }
    }

    public static final class LineItemSpec {
        public final String sku;
        public final String name;
        public final int quantity;
        public final double unitPrice;
        public final double discount;

        public LineItemSpec(String sku, String name, int quantity, double unitPrice, double discount) {
            this.sku = sku;
            this.name = name;
            this.quantity = quantity;
            this.unitPrice = unitPrice;
            this.discount = discount;
        }
    }
}
