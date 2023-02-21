package eu.europeana.cloud.service.dps.utils;

import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;


public class TopologiesTopicsParserTest {

  private static String VALID_INPUT = "oai_topology:oai_topology_1,oai_topology_2,oai_topology_3;another_topology:topic1,topic2";
  private static String VALID_INPUT_1 = "oai_topology:oai_topology_1,oai_topology_2,oai_topology_3;another_topology:";
  private static String INVALID_INPUT_1 = "invalid_topics_list";

  @Test
  public void shouldSuccessfullyParseTopicsList() {
    TopologiesTopicsParser t = new TopologiesTopicsParser();
    Map<String, List<String>> topologiesTopicsList = t.parse(VALID_INPUT);
    Assert.assertEquals(2, topologiesTopicsList.size());
    Assert.assertNotNull(topologiesTopicsList.get("oai_topology"));
    Assert.assertNotNull(topologiesTopicsList.get("another_topology"));

    Assert.assertEquals(3, topologiesTopicsList.get("oai_topology").size());
    Assert.assertEquals(2, topologiesTopicsList.get("another_topology").size());
  }

  @Test
  public void shouldSuccessfullyParseTopicsList_1() {
    TopologiesTopicsParser t = new TopologiesTopicsParser();
    Map<String, List<String>> topologiesTopicsList = t.parse(VALID_INPUT_1);
    Assert.assertEquals(2, topologiesTopicsList.size());
    Assert.assertNotNull(topologiesTopicsList.get("oai_topology"));
    Assert.assertNotNull(topologiesTopicsList.get("another_topology"));

    Assert.assertEquals(3, topologiesTopicsList.get("oai_topology").size());
    Assert.assertEquals(0, topologiesTopicsList.get("another_topology").size());
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowExceptionForInvalidTopicsList_1() {
    TopologiesTopicsParser t = new TopologiesTopicsParser();
    t.parse(INVALID_INPUT_1);
  }

}