package eu.europeana.cloud.service.dps.service.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Class manages topology names and topology users
 */
public class TopologyManager {
    public static final String separatorChar = ",";
    private Map<String, String> topologies = new HashMap<>();
    public static final Logger logger = LoggerFactory.getLogger(TopologyManager.class);

    /**
     * Construct {@link eu.europeana.cloud.service.dps.service.utils.TopologyManager}.
     * @param nameList list of topology names (without white characters)
     * @param userNameList list of topology userNames (without white characters)
     */
    public TopologyManager(final String nameList, final String userNameList) {
        String[] names = nameList.split(separatorChar);
        String[] userNames = userNameList.split(separatorChar);
        assertEqualsLength(names, userNames);
        for (int i = 0; i < names.length; i++) {
            topologies.put(names[i],userNames[i]);
        }
        logResult();
    }

    private void logResult() {
        for(Map.Entry<String,String> entry : topologies.entrySet()){
            logger.info("Topology registered -> topologyName=" + entry.getKey() + " topologyUserName=" + entry.getKey());
        }
    }

    /**
     * Method returns list of topology userNames.
     * @return topology userNames
     */
    public List<String> getUserNames() {
        return new ArrayList<>(topologies.values());
    }

    /**
     * Method return list of topology names.
     * @return topology names
     */
    public List<String> getNames() {
        return new ArrayList<>(topologies.keySet());
    }

    /**
     * Method return mapping between topology names and topology users.
     * @return topology mapping name2userName
     */
    public Map<String, String> getNameToUserMap() {
        return topologies;
    }

    private void assertEqualsLength(String[] names, String[] users) {
        checkArgument(names.length == users.length,"Different number of elements on nameList and userNameList.");
    }

    public boolean containsTopology(String topologyName) {
        return topologies.containsKey(topologyName);
    }
}
