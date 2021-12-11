package com.kafka.producer.singapore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class KafkaController {
    //this is the producer for topic singapore
    private final Producer singaporeProducer;
    private RestTemplate restTemplate = new RestTemplate();
    private List<String> topics;

    @PostConstruct
    public void init() {
        topics = new ArrayList<>();
        topics.add("USA");
        topics.add("Malaysia");
    }

    @Autowired
    public KafkaController(Producer singaporeProducer){
        this.singaporeProducer = singaporeProducer;
    }

    @GetMapping("/advertise")
    public List<String> advertise() {
        System.out.println("advertising");
        return topics;
    }

    @PostMapping(path="/deadvertise")
    public List<String> deadvertise() {
        System.out.println("deadvertising");
        topics.clear();
        return topics;
    }

    @PostMapping("/publish/singapore")
    public void messageToTopic() {
        String singaporeMessage = getTopicData();
        this.singaporeProducer.sendMessage(singaporeMessage);
    }

    public String getTopicData() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String urlSingapore = "https://travelbriefing.org/Singapore?format=json";
            String singaporeDataJsonString = restTemplate.getForObject(urlSingapore, String.class);
            Map<String, Object> singaporeDataMap = mapper.readValue(singaporeDataJsonString, Map.class);

            Map<String, Object> singaporeCurrencyObj = (Map<String, Object>) singaporeDataMap.get("currency");
            String singaporeCurrency = String.valueOf(singaporeCurrencyObj.get("rate"));

            Map<String, Object> singaporeAdviseObj = (Map<String, Object>) singaporeDataMap.get("advise");
            Map<String, Object> singaporeUAObj = (Map<String, Object>) singaporeAdviseObj.get("UA");
            Map<String, Object> singaporeCAObj = (Map<String, Object>) singaporeAdviseObj.get("CA");
            Object singaporeUaAdv = singaporeUAObj.get("advise");
            Object singaporeCaAdv = singaporeCAObj.get("advise");
            String singaporeAdviseFinal = String.valueOf(singaporeUaAdv) + ". " + String.valueOf(singaporeCaAdv);

            StringBuilder singaporeFinalVaccinations = new StringBuilder();
            List<Map<String, Object>> singaporeVaccinations = (List<Map<String, Object>>) singaporeDataMap.get("vaccinations");
            for(Map<String, Object> v : singaporeVaccinations) {
                String name = String.valueOf(v.get("name"));
                singaporeFinalVaccinations.append(name+", ");
            }
            singaporeFinalVaccinations.deleteCharAt(singaporeFinalVaccinations.length()-1);
            return "singapore | "+String.valueOf(singaporeFinalVaccinations) +" | "+ singaporeAdviseFinal +" | "+ singaporeCurrency;
        } catch(Exception e) {
        }
        return null;
    }
}
