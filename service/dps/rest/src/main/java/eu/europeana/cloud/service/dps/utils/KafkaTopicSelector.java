package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static eu.europeana.cloud.service.dps.config.JndiNames.*;
@Service
public class KafkaTopicSelector {

    private final Map<String, List<String>> availableTopic;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    public KafkaTopicSelector(Environment environment) {
        availableTopic = new TopologiesTopicsParser().parse(environment.getProperty(JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS));
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
