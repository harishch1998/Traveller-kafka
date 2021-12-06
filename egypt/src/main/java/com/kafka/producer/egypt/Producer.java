package com.kafka.producer.egypt;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {
    //One
    //This is a producer for topic Egypt
    private static final String TOPIC = "egypt";
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    public void sendMessage(String message) {
        this.kafkaTemplate.send(TOPIC, message);
    }
}
