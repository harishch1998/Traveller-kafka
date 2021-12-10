package com.kafka.consumer.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RestController
@CrossOrigin
public class ConsumerController {
    private final Consumer consumer;
    private RestTemplate restTemplate = new RestTemplate();

    @Autowired
    public ConsumerController(Consumer consumer){
        this.consumer = consumer;
    }

    @PostMapping(
            path = "/subscribe",
            consumes = {MediaType.APPLICATION_FORM_URLENCODED_VALUE})
    public void subscribe(String topic1, String topic2, String topic3) {
        List<String> topics = new ArrayList<>();
        if(topic1 != null)
            topics.add(topic1);
        if(topic2 != null)
            topics.add(topic2);
        if(topic3 != null)
            topics.add(topic3);
        System.out.println("# topics are:"+topics.size());
        this.consumer.subscribe(topics);
    }

    @PostMapping(
            path = "/unsubscribe",
            consumes = {MediaType.APPLICATION_FORM_URLENCODED_VALUE})
    public void unsubscribe(String topic1, String topic2, String topic3){
        Set<String> topicSet = new HashSet<>();
        if(topic1 != null)
            topicSet.add(topic1);
        if(topic2 != null)
            topicSet.add(topic2);
        if(topic3 != null)
            topicSet.add(topic3);
        System.out.println("# topics are:"+topicSet.size());
        this.consumer.unsubscribe(topicSet);
    }

    @GetMapping("/consumer/topics")
    public List<String> getTopics() {
        return consumer.getTopics();
    }

    @GetMapping("/consumer/messages")
    public List<String> getMessages() {
        return consumer.getMessages();
    }



}
