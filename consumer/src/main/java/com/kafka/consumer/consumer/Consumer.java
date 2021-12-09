package com.kafka.consumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class Consumer {

    private KafkaConsumer consumer = getConsumer();
    private List<String> messages = new ArrayList<>();
    private Set<String> topics = new HashSet<>();
    public void setTopics(Set<String> topics) {
        this.topics = topics;
    }

    public KafkaConsumer getConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka1:9092, kafka2:9092, kafka3:9092");
        props.setProperty("group.id", "group_10");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public void subscribe(List<String> subTopics){
        //updating topics for consumer
        setTopics(subTopics.stream().collect(Collectors.toSet()));
        StringBuilder sb = new StringBuilder();
        for(String topic : topics)
            sb.append(topic+",");
        System.out.println("Subscribed to: "+sb);
        consumer.subscribe(topics);
    }

    public void unsubscribe(Set<String> topicsToUnsubscribe) {
        Set<String> topicsSubscribed = consumer.subscription();
        List<String> newTopicList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (String topic : topicsSubscribed) {
            sb.append(topic + ",");
            if (!topicsToUnsubscribe.contains(topic)) {
                newTopicList.add(topic);
            }
        }
        System.out.println("TOPICS subscribed:" + sb);
        consumer.unsubscribe();
        subscribe(newTopicList);
        //updating topics for consumer
        topics = newTopicList.stream().collect(Collectors.toSet());

        StringBuilder newList = new StringBuilder();
        Set<String> ts = consumer.subscription();
        for (String topic : ts) {
            newList.append(topic + ",");
        }
        System.out.println("NEW TOPICS subscribed:" + newList);
    }

    @KafkaListener(topics = {"india", "egypt", "singapore"}, groupId = "group_10")
    public void listen(String message) {
        System.out.println("got message using listener: " + message);
        synchronized (messages) {
            String[] splitMessage = message.split("\\|");
            if(topics.contains(splitMessage[0].trim())) {
                System.out.println("filtered messages topic wise to add: " + message);
                messages.add(message);
            }
        }
    }

    public List<String> getMessages() {
        System.out.println("Returning messages: " + messages.size());
        return messages;
    }

}
