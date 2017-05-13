package com.example.demo.model;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class Route
{

    private Map<String, String> message;

    public Route(ConsumerRecord<String, String> consumerRecord)
    {
    }

    public String getMessageKey(String key)
    {
        return message.get(key);
    }
}
