package com.kafka.producer.india;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class Producer {
    //This is a producer for topic India
    private static final String TOPIC = "india";
    private KafkaProducer producer = getProducer();
    public KafkaProducer getProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "kafka1:9092, kafka2:9092, kafka3:9092");
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 0);
        producerProperties.put("batch.size", 16384);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        return producer;
    }
    public void sendMessage(String message) {
        producer.send(new ProducerRecord<>(TOPIC, message));
    }
}
