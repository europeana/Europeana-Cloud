package eu.europeana.cloud.common.model.dps;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;

@XmlRootElement
/**
 * Class for statistics of a single record
 */
public class RecordStatistics {
    /** Task identifier */
    private final long taskId;

    /** Map associating nodes with their occurence */
    private final Map<NodeStatistics, Integer> nodesMap = new HashMap<>();

    public RecordStatistics(long taskId) {
        this.taskId = taskId;
    }

    public long getTaskId() {
        return taskId;
    }

    /**
     * Retrieve a copy of the node statistics map
     *
     * @return copy of the map
     */
    public Map<NodeStatistics, Integer> getNodesMap() {
        Map<NodeStatistics, Integer> copy = new HashMap<>(nodesMap);
        return copy;
    }

    /**
     * Add occurrence for the specified node. When adding for the first time it will get occurrence 1
     * otherwise the occurrence will be increased by 1
     *
     * @param nodeStatistics added node
     */
    public void addNodeStatistics(NodeStatistics nodeStatistics) {
        Integer count = nodesMap.get(nodeStatistics);
        if (count == null) {
            nodesMap.put(nodeStatistics, Integer.valueOf(1));
        } else {
            nodesMap.put(nodeStatistics, Integer.valueOf(count + 1));
        }
    }

    /**
     * Retrieve occurrence for the specified node.
     *
     * @param nodeStatistics node statistics object
     * @return occurrence for the specified node
     */
    public int getNodeStatisticsCount(NodeStatistics nodeStatistics) {
        Integer count = nodesMap.get(nodeStatistics);
        if (count == null) {
            return 0;
        }
        return count;
    }

    /**
     * Retrieve node statistics object for the specified xpath.
     *
     * @param xpath node xpath to search for
     * @return NodeStatistics object that has given xpath
     */
    public NodeStatistics getNodeStatistics(String xpath) {
        for (NodeStatistics stats : nodesMap.keySet()) {
            if (stats.getXpath().equals(xpath)) {
                return stats;
            }
        }
        return null;
    }
}
