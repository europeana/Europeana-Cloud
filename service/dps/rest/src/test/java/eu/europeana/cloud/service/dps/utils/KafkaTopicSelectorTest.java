package eu.europeana.cloud.service.dps.utils;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.collect.HashMultiset;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.service.dps.properties.KafkaProperties;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaTopicSelectorTest {

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

  @Before
  public void setup() {
    when(kafkaProperties.getTopologyAvailableTopics()).thenReturn(TOPIC_CONFIG);
    selector = new KafkaTopicSelector(tasksByStateDAO, taskStatusSynchronizer, kafkaProperties);
    selectionCounts = HashMultiset.create();
  }

  @Test
  public void shouldUseAllTopicsWithTheSameFrequencyIfAllOfThemAreFree() {
    when(tasksByStateDAO.findTasksByStateAndTopology(anyList(), anyString())).thenReturn(new ArrayList<>());

    selectTopics();

    assertEquals(0.25, freqency(TOPIC_1), TOLERANCE);
    assertEquals(0.25, freqency(TOPIC_2), TOLERANCE);
    assertEquals(0.25, freqency(TOPIC_3), TOLERANCE);
    assertEquals(0.25, freqency(TOPIC_4), TOLERANCE);
  }

  @Test
  public void shouldUseAllRemainingFreeTopicsWithTheSameFrequency() {
    when(tasksByStateDAO.findTasksByStateAndTopology(anyList(), anyString())).thenReturn(
        Arrays.asList(createTaskByTaskState(TOPIC_1), createTaskByTaskState(TOPIC_3)));

    selectTopics();

    assertEquals(0.0, freqency(TOPIC_1));
    assertEquals(0.5, freqency(TOPIC_2), TOLERANCE);
    assertEquals(0.0, freqency(TOPIC_3));
    assertEquals(0.5, freqency(TOPIC_4), TOLERANCE);
  }

  @Test
  public void shouldWorkProperlyWhereTasksWithStillUnselectedTopicExist() {
    when(tasksByStateDAO.findTasksByStateAndTopology(anyList(), anyString())).thenReturn(
        Arrays.asList(createTaskByTaskState(null), createTaskByTaskState(TOPIC_2),
            createTaskByTaskState(null)));

    selectTopics();

    assertEquals(0.3333, freqency(TOPIC_1), TOLERANCE);
    assertEquals(0.0, freqency(TOPIC_2));
    assertEquals(0.3333, freqency(TOPIC_3), TOLERANCE);
    assertEquals(0.3333, freqency(TOPIC_4), TOLERANCE);
  }

  @Test
  public void shouldUseAllTopicsWithTheSameFrequencyIfAllTopicsAreUsed() {
    when(tasksByStateDAO.findTasksByStateAndTopology(anyList(), anyString())).thenReturn(
        Arrays.asList(createTaskByTaskState(TOPIC_1), createTaskByTaskState(TOPIC_2),
            createTaskByTaskState(TOPIC_3), createTaskByTaskState(TOPIC_4)));

    selectTopics();

    assertEquals(0.25, freqency(TOPIC_1), TOLERANCE);
    assertEquals(0.25, freqency(TOPIC_2), TOLERANCE);
    assertEquals(0.25, freqency(TOPIC_3), TOLERANCE);
    assertEquals(0.25, freqency(TOPIC_4), TOLERANCE);
  }

  private void selectTopics() {
    for (int i = 0; i < SAMPLES_COUNT; i++) {
      selectionCounts.add(selector.findPreferredTopicNameFor(TOPOLOGY));
    }
  }

  private double freqency(String topic) {
    return (double) selectionCounts.count(topic) / SAMPLES_COUNT;
  }

  private TaskByTaskState createTaskByTaskState(String topic) {
    return TaskByTaskState.builder().topologyName(TOPOLOGY).topicName(topic).build();
  }
}