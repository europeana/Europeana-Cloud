package eu.europeana.cloud.service.dps.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;


public class TopologiesTopicsParserTest {

    private static String VALID_INPUT = "oai_topology:oai_topology_1,oai_topology_2,oai_topology_3;another_topology:topic1,topic2";
    private static String VALID_INPUT_1 = "oai_topology:oai_topology_1,oai_topology_2,oai_topology_3;another_topology:";
    private static String INVALID_INPUT_1 = "invalid_topics_list";

    @Test
    public void shouldSuccessfullyParseTopicsList() {
        TopologiesTopicsParser t = new TopologiesTopicsParser();
        Map<String, List<String>> topologiesTopicsList = t.parse(VALID_INPUT);
        Assert.assertTrue(topologiesTopicsList.size() == 2);
        Assert.assertTrue(topologiesTopicsList.get("oai_topology") != null);
        Assert.assertTrue(topologiesTopicsList.get("another_topology") != null);

        Assert.assertTrue(topologiesTopicsList.get("oai_topology").size() == 3);
        Assert.assertTrue(topologiesTopicsList.get("another_topology").size() == 2);
    }

    @Test
    public void shouldSuccessfullyParseTopicsList_1() {
        TopologiesTopicsParser t = new TopologiesTopicsParser();
        Map<String, List<String>> topologiesTopicsList = t.parse(VALID_INPUT_1);
        Assert.assertTrue(topologiesTopicsList.size() == 2);
        Assert.assertTrue(topologiesTopicsList.get("oai_topology") != null);
        Assert.assertTrue(topologiesTopicsList.get("another_topology") != null);

        Assert.assertTrue(topologiesTopicsList.get("oai_topology").size() == 3);
        Assert.assertTrue(topologiesTopicsList.get("another_topology").size() == 0);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForInvalidTopicsList_1() {
        TopologiesTopicsParser t = new TopologiesTopicsParser();
        t.parse(INVALID_INPUT_1);
    }

}