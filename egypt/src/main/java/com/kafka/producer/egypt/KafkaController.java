package com.kafka.producer.egypt;

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
    //this is the producer for topic egypt
    private final Producer egyptProducer;
    private RestTemplate restTemplate = new RestTemplate();
    private List<String> topics;

    @PostConstruct
    public void init() {
        topics = new ArrayList<>();
        topics.add("USA");
        topics.add("Malaysia");
    }

    @Autowired
    public KafkaController(Producer egyptProducer){
        this.egyptProducer = egyptProducer;
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

    @PostMapping("/publish/egypt")
    public void messageToTopic() {
        String egyptMessage = getTopicData();
        this.egyptProducer.sendMessage(egyptMessage);
    }

    public String getTopicData() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String urlEgypt = "https://travelbriefing.org/Egypt?format=json";
            String egyptDataJsonString = restTemplate.getForObject(urlEgypt, String.class);
            Map<String, Object> egyptDataMap = mapper.readValue(egyptDataJsonString, Map.class);

            Map<String, Object> egyptCurrencyObj = (Map<String, Object>) egyptDataMap.get("currency");
            String egyptCurrency = String.valueOf(egyptCurrencyObj.get("rate"));

            Map<String, Object> egyptAdviseObj = (Map<String, Object>) egyptDataMap.get("advise");
            Map<String, Object> egyptUAObj = (Map<String, Object>) egyptAdviseObj.get("UA");
            Map<String, Object> egyptCAObj = (Map<String, Object>) egyptAdviseObj.get("CA");
            Object egyptUaAdv = egyptUAObj.get("advise");
            Object egyptCaAdv = egyptCAObj.get("advise");
            String egyptAdviseFinal = String.valueOf(egyptUaAdv) + ". " + String.valueOf(egyptCaAdv);

            StringBuilder egyptFinalVaccinations = new StringBuilder();
            List<Map<String, Object>> egyptVaccinations = (List<Map<String, Object>>) egyptDataMap.get("vaccinations");
            for(Map<String, Object> v : egyptVaccinations) {
                String name = String.valueOf(v.get("name"));
                egyptFinalVaccinations.append(name+", ");
            }
            egyptFinalVaccinations.deleteCharAt(egyptFinalVaccinations.length()-1);
            return "egypt | "+ String.valueOf(egyptFinalVaccinations) +" | "+ egyptAdviseFinal +" | "+ egyptCurrency;
        } catch(Exception e) {
        }
        return null;
    }
}
