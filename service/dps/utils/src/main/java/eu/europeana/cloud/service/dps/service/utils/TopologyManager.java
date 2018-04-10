package eu.europeana.cloud.service.dps.service.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Class manages topology names and topology users
 */
public class TopologyManager {
    public static final String separatorChar = ",";
    private List<String> topologies = new ArrayList<>();
    public static final Logger logger = LoggerFactory.getLogger(TopologyManager.class);

    /**
     * Construct {@link eu.europeana.cloud.service.dps.service.utils.TopologyManager}.
     *
     * @param nameList list of topology names (without white characters)
     */
    public TopologyManager(final String nameList) {
        String[] names = nameList.split(separatorChar);
        assertNotEmpty(names);
        for (int i = 0; i < names.length; i++) {
            topologies.add(names[i]);
        }
        logResult();
    }

    private void logResult() {
        for (String topologyName : topologies) {
            logger.info("Topology registered -> topologyName=" + topologyName);
        }
    }

    /**
     * Method return list of topology names.
     *
     * @return topology names
     */
    public List<String> getNames() {
        return topologies;
    }


    private void assertNotEmpty(String[] names) {
        checkArgument(names.length > 0, "No registered topologies");
    }

    public boolean containsTopology(String topologyName) {
        return topologies.contains(topologyName);
    }
}
