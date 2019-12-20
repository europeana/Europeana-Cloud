package eu.europeana.cloud.service.dps.utils;

import java.util.*;

public class TopologiesTopicsParser {

    private static final String INPUT_VALUE_REGEX = "(\\w+:(\\w*,?)+;?)+";

    public Map<String, List<String>> parse(String topicsList) {
        if (isInputValid(topicsList)) {
            List<String> topologies = extractTopologies(topicsList);
            return extractTopics(topologies);
        } else {
            throw new RuntimeException("Topics list is not valid");
        }
    }

    private boolean isInputValid(String inputTopicsList) {
        return inputTopicsList.matches(INPUT_VALUE_REGEX);
    }

    private List<String> extractTopologies(String inputTopicsList) {
        List<String> topologies = new ArrayList<>();
        Scanner scanner = new Scanner(inputTopicsList).useDelimiter(";");
        while (scanner.hasNext()) {
            topologies.add(scanner.next());
        }
        return topologies;
    }

    private Map<String, List<String>> extractTopics(List<String> topologies) {
        Map<String, List<String>> resultsMap = new HashMap<>();

        for (String topology : topologies) {
            Scanner scanner = new Scanner(topology).useDelimiter(":");
            String topologyName = scanner.next();
            resultsMap.put(topologyName, Collections.EMPTY_LIST);
            while (scanner.hasNext()) {
                resultsMap.put(topologyName, Arrays.asList(scanner.next().split(",")));
            }
        }
        return resultsMap;
    }
}
