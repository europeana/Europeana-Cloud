package eu.europeana.cloud.service.dps.utils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


class TopologiesTopicsParserTest {

    private static String VALID_INPUT = "oai_topology:oai_topology_1,oai_topology_2,oai_topology_3;another_topology:topic1,topic2";
    private static String VALID_INPUT_1 = "oai_topology:oai_topology_1,oai_topology_2,oai_topology_3;another_topology:";
    private static String INVALID_INPUT_1 = "invalid_topics_list";

    @Test
    void shouldSuccessfullyParseTopicsList() {
        TopologiesTopicsParser t = new TopologiesTopicsParser();
        Map<String, List<String>> topologiesTopicsList = t.parse(VALID_INPUT);
        assertEquals(2, topologiesTopicsList.size());
        assertNotNull(topologiesTopicsList.get("oai_topology"));
        assertNotNull(topologiesTopicsList.get("another_topology"));

        assertEquals(3, topologiesTopicsList.get("oai_topology").size());
        assertEquals(2, topologiesTopicsList.get("another_topology").size());
    }

    @Test
    void shouldSuccessfullyParseTopicsList_1() {
        TopologiesTopicsParser t = new TopologiesTopicsParser();
        Map<String, List<String>> topologiesTopicsList = t.parse(VALID_INPUT_1);
        assertEquals(2, topologiesTopicsList.size());
        assertNotNull(topologiesTopicsList.get("oai_topology"));
        assertNotNull(topologiesTopicsList.get("another_topology"));

        assertEquals(3, topologiesTopicsList.get("oai_topology").size());
        assertEquals(0, topologiesTopicsList.get("another_topology").size());
    }

    @Test
    void shouldThrowExceptionForInvalidTopicsList_1() {
        TopologiesTopicsParser t = new TopologiesTopicsParser();
        assertThrows(RuntimeException.class, () -> t.parse(INVALID_INPUT_1));
    }

}