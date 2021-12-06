package com.kafka.producer.singapore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {
    //This is a producer for topic Singapore
    private static final String TOPIC = "singapore";
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    public void sendMessage(String message) {
        this.kafkaTemplate.send(TOPIC, message);
    }
}
