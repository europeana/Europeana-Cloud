package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.config.JndiNames.JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS;

@Service
public class KafkaTopicSelector {

  private final Map<String, List<String>> availableTopic;

  private final Random rand;
  private TasksByStateDAO tasksByStateDAO;

  private TaskStatusSynchronizer taskStatusSynchronizer;


  public KafkaTopicSelector(Environment environment, TasksByStateDAO tasksByStateDAO,
      TaskStatusSynchronizer taskStatusSynchronizer) {
    availableTopic = Collections.unmodifiableMap(
        new TopologiesTopicsParser().parse(environment.getProperty(JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS)));
    this.tasksByStateDAO = tasksByStateDAO;
    this.taskStatusSynchronizer = taskStatusSynchronizer;
    this.rand = new Random();
  }

  public String findPreferredTopicNameFor(String topologyName) {
    return randomFreeTopic(topologyName)
        .orElseGet(() -> {
              synchronizeTasksByTaskStateFromBasicInfo(topologyName);
              return randomFreeTopic(topologyName)
                  .orElse(randomTopic(topologyName));
            }
        );
  }

  private void synchronizeTasksByTaskStateFromBasicInfo(String topologyName) {
    taskStatusSynchronizer.synchronizeTasksByTaskStateFromBasicInfo(topologyName, availableTopic.get(topologyName));
  }

  private Optional<String> randomFreeTopic(String topologyName) {
    List<String> freeTopics = findAllFreeTopics(topologyName);
    if (freeTopics.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(freeTopics.get(rand.nextInt(freeTopics.size())));
    }
  }

  private List<String> findAllFreeTopics(String topologyName) {
    List<String> freeTopics = new ArrayList<>(topicsForOneTopology(topologyName));
    freeTopics.removeAll(findTopicsCurrentlyInUse(topologyName));
    return freeTopics;
  }

  private Set<String> findTopicsCurrentlyInUse(String topologyName) {
    return tasksByStateDAO.findTasksByStateAndTopology(
                              Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION, TaskState.QUEUED), topologyName)
                          .stream()
                          .map(TaskByTaskState::getTopicName)
                          .filter(Objects::nonNull)
                          .collect(Collectors.toSet());
  }

  private String randomTopic(String topologyName) {
    List<String> topicsForOneTopology = topicsForOneTopology(topologyName);
    return topicsForOneTopology.get(rand.nextInt(topicsForOneTopology.size()));
  }

  private List<String> topicsForOneTopology(String topologyName) {
    return availableTopic.get(topologyName);
  }
}
