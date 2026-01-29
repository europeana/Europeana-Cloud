package eu.europeana.cloud.service.dps.utils;

import com.google.common.collect.HashMultiset;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.service.dps.properties.KafkaProperties;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaTopicSelectorTest {

    private static final int SAMPLES_COUNT = 10_000;
    private static final double TOLERANCE = 0.02;
    private static final String TOPIC_CONFIG = "topology:topic_1,topic_2,topic_3,topic_4";
    private static final String TOPOLOGY = "topology";
    private static final String TOPIC_1 = "topic_1";
    private static final String TOPIC_2 = "topic_2";
    private static final String TOPIC_3 = "topic_3";
    private static final String TOPIC_4 = "topic_4";


  @Mock
  private KafkaProperties kafkaProperties;

    @Mock
    private TasksByStateDAO tasksByStateDAO;

    @Mock
    private TaskStatusSynchronizer taskStatusSynchronizer;

    private KafkaTopicSelector selector;
    private HashMultiset<String> selectionCounts;

    @BeforeEach
    void setup() {
        when(kafkaProperties.getTopologyAvailableTopics()).thenReturn(TOPIC_CONFIG);
        selector = new KafkaTopicSelector(tasksByStateDAO, taskStatusSynchronizer, kafkaProperties);
        selectionCounts = HashMultiset.create();
    }

    @Test
    public void shouldUseAllTopicsWithTheSameFrequencyIfAllOfThemAreFree() {
        when(tasksByStateDAO.findTasksByStateAndTopology(anyList(), anyString())).thenReturn(new ArrayList<>());

        selectTopics();

        assertEquals(0.25, frequency(TOPIC_1), TOLERANCE);
        assertEquals(0.25, frequency(TOPIC_2), TOLERANCE);
        assertEquals(0.25, frequency(TOPIC_3), TOLERANCE);
        assertEquals(0.25, frequency(TOPIC_4), TOLERANCE);
    }

    @Test
    void shouldUseAllRemainingFreeTopicsWithTheSameFrequency() {
        when(tasksByStateDAO.findTasksByStateAndTopology(anyList(), anyString())).thenReturn(
                Arrays.asList(createTaskByTaskState(TOPIC_1), createTaskByTaskState(TOPIC_3)));

        selectTopics();

        assertEquals(0.0, frequency(TOPIC_1));
        assertEquals(0.5, frequency(TOPIC_2), TOLERANCE);
        assertEquals(0.0, frequency(TOPIC_3));
        assertEquals(0.5, frequency(TOPIC_4), TOLERANCE);
    }

    @Test
    void shouldWorkProperlyWhereTasksWithStillUnselectedTopicExist() {
        when(tasksByStateDAO.findTasksByStateAndTopology(anyList(), anyString())).thenReturn(
                Arrays.asList(createTaskByTaskState(null), createTaskByTaskState(TOPIC_2),
                        createTaskByTaskState(null)));

        selectTopics();

        assertEquals(0.3333, frequency(TOPIC_1), TOLERANCE);
        assertEquals(0.0, frequency(TOPIC_2));
        assertEquals(0.3333, frequency(TOPIC_3), TOLERANCE);
        assertEquals(0.3333, frequency(TOPIC_4), TOLERANCE);
    }

    @Test
    void shouldUseAllTopicsWithTheSameFrequencyIfAllTopicsAreUsed() {
        when(tasksByStateDAO.findTasksByStateAndTopology(anyList(), anyString())).thenReturn(
                Arrays.asList(createTaskByTaskState(TOPIC_1), createTaskByTaskState(TOPIC_2),
                        createTaskByTaskState(TOPIC_3), createTaskByTaskState(TOPIC_4)));

        selectTopics();

        assertEquals(0.25, frequency(TOPIC_1), TOLERANCE);
        assertEquals(0.25, frequency(TOPIC_2), TOLERANCE);
        assertEquals(0.25, frequency(TOPIC_3), TOLERANCE);
        assertEquals(0.25, frequency(TOPIC_4), TOLERANCE);
    }

  private void selectTopics() {
    for (int i = 0; i < SAMPLES_COUNT; i++) {
      selectionCounts.add(selector.findPreferredTopicNameFor(TOPOLOGY));
    }
  }

    private double frequency(String topic) {
        return (double) selectionCounts.count(topic) / SAMPLES_COUNT;
    }

  private TaskByTaskState createTaskByTaskState(String topic) {
    return TaskByTaskState.builder().topologyName(TOPOLOGY).topicName(topic).build();
  }
}