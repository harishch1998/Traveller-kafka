package com.kafka.producer.india;

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
    //this is the producer for topic india
    private final Producer indiaProducer;
    private RestTemplate restTemplate = new RestTemplate();
    private List<String> topics;

    @PostConstruct
    public void init() {
        topics = new ArrayList<>();
        topics.add("USA");
        topics.add("Malaysia");
    }

    @Autowired
    public KafkaController(Producer indiaProducer){
        this.indiaProducer = indiaProducer;
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

    @PostMapping("/publish/india")
    public void messageToTopic() {
        String indiaMessage = getTopicData();
        this.indiaProducer.sendMessage(indiaMessage);
    }

    public String getTopicData() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String urlIndia = "https://travelbriefing.org/India?format=json";
            String indiaDataJsonString = restTemplate.getForObject(urlIndia, String.class);
            Map<String, Object> indiaDataMap = mapper.readValue(indiaDataJsonString, Map.class);

            Map<String, Object> indiaCurrencyObj = (Map<String, Object>) indiaDataMap.get("currency");
            String indiaCurrency = String.valueOf(indiaCurrencyObj.get("rate"));

            Map<String, Object> indiaAdviseObj = (Map<String, Object>) indiaDataMap.get("advise");
            Map<String, Object> indiaUAObj = (Map<String, Object>) indiaAdviseObj.get("UA");
            Map<String, Object> indiaCAObj = (Map<String, Object>) indiaAdviseObj.get("CA");
            Object indiaUaAdv = indiaUAObj.get("advise");
            Object indiaCaAdv = indiaCAObj.get("advise");
            String indiaAdviseFinal = String.valueOf(indiaUaAdv) + ". " + String.valueOf(indiaCaAdv);

            StringBuilder indiaFinalVaccinations = new StringBuilder();
            List<Map<String, Object>> indiaVaccinations = (List<Map<String, Object>>) indiaDataMap.get("vaccinations");
            for(Map<String, Object> v : indiaVaccinations) {
                String name = String.valueOf(v.get("name"));
                indiaFinalVaccinations.append(name+", ");
            }
            indiaFinalVaccinations.deleteCharAt(indiaFinalVaccinations.length()-1);
            return "india | "+String.valueOf(indiaFinalVaccinations) + " | " + indiaAdviseFinal + " | " + indiaCurrency;
        } catch(Exception e) {
        }
        return null;
    }
}
