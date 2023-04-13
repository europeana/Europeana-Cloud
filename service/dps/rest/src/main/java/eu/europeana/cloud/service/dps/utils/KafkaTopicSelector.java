package eu.europeana.cloud.service.dps.utils;


import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.properties.KafkaProperties;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaTopicSelector {

  private final Map<String, List<String>> availableTopic;

  @SuppressWarnings("java:S2245") //Random is used here only for even usage of all topics. So the usage is secure.
  private final Random random;
  private TasksByStateDAO tasksByStateDAO;

  private TaskStatusSynchronizer taskStatusSynchronizer;


  @Autowired
  public KafkaTopicSelector(TasksByStateDAO tasksByStateDAO,
      TaskStatusSynchronizer taskStatusSynchronizer,
      KafkaProperties kafkaProperties) {
    availableTopic = Collections.unmodifiableMap(
        new TopologiesTopicsParser().parse(kafkaProperties.getTopologyAvailableTopics()));
    this.tasksByStateDAO = tasksByStateDAO;
    this.taskStatusSynchronizer = taskStatusSynchronizer;
    this.random = new Random();
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
      return Optional.of(freeTopics.get(random.nextInt(freeTopics.size())));
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
    return topicsForOneTopology.get(random.nextInt(topicsForOneTopology.size()));
  }

  private List<String> topicsForOneTopology(String topologyName) {
    return availableTopic.get(topologyName);
  }
}
