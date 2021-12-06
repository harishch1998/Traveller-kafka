package com.kafka.consumer.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RestController
public class ConsumerController {
    private final Consumer consumer;
    private RestTemplate restTemplate = new RestTemplate();

    @Autowired
    public ConsumerController(Consumer consumer){
        this.consumer = consumer;
    }

    @PostMapping("/subscribe")
    public void subscribe(@RequestBody List<String> topics) {
       this.consumer.subscribe(topics);
    }

    @PostMapping("/unsubscribe")
    public void unsubscribe(@RequestBody List<String> topics){
        Set<String> topicSet = new HashSet<String>(topics);
        this.consumer.unsubscribe(topicSet);
    }

    @GetMapping("/consumer/messages")
    public List<String> getMessages() {
        return consumer.getMessages();
    }

}
