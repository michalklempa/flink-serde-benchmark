package com.michalklempa.flink.serde.benchmark.data;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public final class Mappers {
    private Mappers() {
    }

    public static com.michalklempa.flink.serde.benchmark.pojo.Order toPojo(OrderSpec s) {
        com.michalklempa.flink.serde.benchmark.pojo.Order o = new com.michalklempa.flink.serde.benchmark.pojo.Order();
        o.orderId = s.orderId;
        o.createdAtMillis = s.createdAtMillis;
        o.updatedAtMillis = s.updatedAtMillis;
        o.status = com.michalklempa.flink.serde.benchmark.pojo.OrderStatus.values()[s.status];
        o.customer = new com.michalklempa.flink.serde.benchmark.pojo.Customer();
        o.customer.customerId = s.customer.customerId;
        o.customer.name = s.customer.name;
        o.customer.email = s.customer.email;
        o.customer.vip = s.customer.vip;
        o.customer.address = new com.michalklempa.flink.serde.benchmark.pojo.Address();
        o.customer.address.street = s.customer.address.street;
        o.customer.address.city = s.customer.address.city;
        o.customer.address.country = s.customer.address.country;
        o.customer.address.postalCode = s.customer.address.postalCode;
        o.items = new ArrayList<>(s.items.size());
        for (OrderSpec.LineItemSpec i : s.items) {
            com.michalklempa.flink.serde.benchmark.pojo.LineItem li = new com.michalklempa.flink.serde.benchmark.pojo.LineItem();
            li.sku = i.sku;
            li.name = i.name;
            li.quantity = i.quantity;
            li.unitPrice = i.unitPrice;
            li.discount = i.discount;
            o.items.add(li);
        }
        o.tags = new ArrayList<>(s.tags);
        o.attributes = new HashMap<>(s.attributes);
        o.totalAmount = s.totalAmount;
        o.payload = s.payload.clone();
        return o;
    }

    public static com.michalklempa.flink.serde.benchmark.pojoplain.Order toPojoPlain(OrderSpec s) {
        com.michalklempa.flink.serde.benchmark.pojoplain.Order o = new com.michalklempa.flink.serde.benchmark.pojoplain.Order();
        o.orderId = s.orderId;
        o.createdAtMillis = s.createdAtMillis;
        o.updatedAtMillis = s.updatedAtMillis;
        o.status = com.michalklempa.flink.serde.benchmark.pojoplain.OrderStatus.values()[s.status];
        o.customer = new com.michalklempa.flink.serde.benchmark.pojoplain.Customer();
        o.customer.customerId = s.customer.customerId;
        o.customer.name = s.customer.name;
        o.customer.email = s.customer.email;
        o.customer.vip = s.customer.vip;
        o.customer.address = new com.michalklempa.flink.serde.benchmark.pojoplain.Address();
        o.customer.address.street = s.customer.address.street;
        o.customer.address.city = s.customer.address.city;
        o.customer.address.country = s.customer.address.country;
        o.customer.address.postalCode = s.customer.address.postalCode;
        o.items = new ArrayList<>(s.items.size());
        for (OrderSpec.LineItemSpec i : s.items) {
            com.michalklempa.flink.serde.benchmark.pojoplain.LineItem li = new com.michalklempa.flink.serde.benchmark.pojoplain.LineItem();
            li.sku = i.sku;
            li.name = i.name;
            li.quantity = i.quantity;
            li.unitPrice = i.unitPrice;
            li.discount = i.discount;
            o.items.add(li);
        }
        o.tags = new ArrayList<>(s.tags);
        o.attributes = new HashMap<>(s.attributes);
        o.totalAmount = s.totalAmount;
        o.payload = s.payload.clone();
        return o;
    }

    public static com.michalklempa.flink.serde.benchmark.avro.Order toAvro(OrderSpec s) {
        List<com.michalklempa.flink.serde.benchmark.avro.LineItem> items = new ArrayList<>(s.items.size());
        for (OrderSpec.LineItemSpec i : s.items) {
            items.add(com.michalklempa.flink.serde.benchmark.avro.LineItem.newBuilder()
                    .setSku(i.sku)
                    .setName(i.name)
                    .setQuantity(i.quantity)
                    .setUnitPrice(i.unitPrice)
                    .setDiscount(i.discount)
                    .build());
        }
        return com.michalklempa.flink.serde.benchmark.avro.Order.newBuilder()
                .setOrderId(s.orderId)
                .setCreatedAtMillis(s.createdAtMillis)
                .setUpdatedAtMillis(s.updatedAtMillis)
                .setStatus(com.michalklempa.flink.serde.benchmark.avro.OrderStatus.values()[s.status])
                .setCustomer(com.michalklempa.flink.serde.benchmark.avro.Customer.newBuilder()
                        .setCustomerId(s.customer.customerId)
                        .setName(s.customer.name)
                        .setEmail(s.customer.email)
                        .setVip(s.customer.vip)
                        .setAddress(com.michalklempa.flink.serde.benchmark.avro.Address.newBuilder()
                                .setStreet(s.customer.address.street)
                                .setCity(s.customer.address.city)
                                .setCountry(s.customer.address.country)
                                .setPostalCode(s.customer.address.postalCode)
                                .build())
                        .build())
                .setItems(items)
                .setTags(new ArrayList<>(s.tags))
                .setAttributes(new HashMap<>(s.attributes))
                .setTotalAmount(s.totalAmount)
                .setPayload(ByteBuffer.wrap(s.payload.clone()))
                .build();
    }

    public static com.michalklempa.flink.serde.benchmark.proto.Order toProto(OrderSpec s) {
        com.michalklempa.flink.serde.benchmark.proto.Order.Builder b = com.michalklempa.flink.serde.benchmark.proto.Order.newBuilder()
                .setOrderId(s.orderId)
                .setCreatedAtMillis(s.createdAtMillis)
                .setUpdatedAtMillis(s.updatedAtMillis)
                .setStatus(com.michalklempa.flink.serde.benchmark.proto.OrderStatus.forNumber(s.status))
                .setCustomer(com.michalklempa.flink.serde.benchmark.proto.Customer.newBuilder()
                        .setCustomerId(s.customer.customerId)
                        .setName(s.customer.name)
                        .setEmail(s.customer.email)
                        .setVip(s.customer.vip)
                        .setAddress(com.michalklempa.flink.serde.benchmark.proto.Address.newBuilder()
                                .setStreet(s.customer.address.street)
                                .setCity(s.customer.address.city)
                                .setCountry(s.customer.address.country)
                                .setPostalCode(s.customer.address.postalCode)))
                .addAllTags(s.tags)
                .putAllAttributes(s.attributes)
                .setTotalAmount(s.totalAmount)
                .setPayload(ByteString.copyFrom(s.payload));
        for (OrderSpec.LineItemSpec i : s.items) {
            b.addItems(com.michalklempa.flink.serde.benchmark.proto.LineItem.newBuilder()
                    .setSku(i.sku)
                    .setName(i.name)
                    .setQuantity(i.quantity)
                    .setUnitPrice(i.unitPrice)
                    .setDiscount(i.discount));
        }
        return b.build();
    }
}
