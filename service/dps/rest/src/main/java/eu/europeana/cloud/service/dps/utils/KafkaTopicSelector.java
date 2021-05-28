package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static eu.europeana.cloud.service.dps.config.JndiNames.*;
@Service
public class KafkaTopicSelector {

    private final Map<String, List<String>> availableTopic;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    @Autowired
    private TaskStatusSynchronizer taskStatusSynchronizer;

    public KafkaTopicSelector(Environment environment) {
        availableTopic = new TopologiesTopicsParser().parse(environment.getProperty(JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS));
    }

    public String findPreferredTopicNameFor(String topologyName) {
        return findFreeTopic(topologyName)
                .orElseGet(() -> {
                            synchronizeTasksByTaskStateFromBasicInfo(topologyName);
                            return findFreeTopic(topologyName)
                                    .orElse(randomTopic(topologyName));
                        }
                );
    }

    private void synchronizeTasksByTaskStateFromBasicInfo(String topologyName) {
        taskStatusSynchronizer.synchronizeTasksByTaskStateFromBasicInfo(topologyName, availableTopic.get(topologyName));
    }

    private Optional<String> findFreeTopic(String topologyName) {
        Set<String> topicsCurrentlyInUse = tasksByStateDAO.listAllInUseTopicsFor(topologyName);
        for (String topicName : topicsForOneTopology(topologyName)) {
            if (!topicsCurrentlyInUse.contains(topicName))
                return Optional.of(topicName);
        }
        return Optional.empty();
    }

    private String randomTopic(String topologyName) {
        List<String> topicsForOneTopology = topicsForOneTopology(topologyName);
        return topicsForOneTopology.get(new Random().nextInt(topicsForOneTopology.size()));
    }

    private List<String> topicsForOneTopology(String topologyName) {
        return availableTopic.get(topologyName);
    }
}
