package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class KafkaTopicSelector {

    private Map<String, List<String>> availableTopic;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    public KafkaTopicSelector(String topologiesTopics) {
        availableTopic = new TopologiesTopicsParser().parse(topologiesTopics);
    }

    public String findPreferredTopicNameFor(String topologyName) {
        List<String> topicsCurrentlyInUse = tasksByStateDAO.listAllInUseTopicsFor(topologyName);

        for (String topicName : availableTopic.get(topologyName)) {
            if (!topicsCurrentlyInUse.contains(topicName))
                return topicName;
        }
        return availableTopic.get(topologyName).get(new Random().nextInt(availableTopic.size()));
    }
}
