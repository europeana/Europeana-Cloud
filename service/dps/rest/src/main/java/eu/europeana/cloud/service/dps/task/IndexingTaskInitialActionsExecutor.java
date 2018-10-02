package eu.europeana.cloud.service.dps.task;

import eu.europeana.cloud.service.dps.DpsTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes all preliminary jobs related with tasks submitted for indexing topology.
 * <p>
 * Created by pwozniak on 10/2/18
 */
public class IndexingTaskInitialActionsExecutor implements TaskInitialActionsExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTaskInitialActionsExecutor.class);

    private DpsTask dpsTask;
    private String topologyName;

    public IndexingTaskInitialActionsExecutor(DpsTask task, String topologyName) {
        this.dpsTask = task;
        this.topologyName = topologyName;
    }

    @Override
    public void execute() {
        LOGGER.info("Executing initial actions for indexing topology");
        removeDataSet("sample");
    }

    private void removeDataSet(String datasetName) {
        LOGGER.info("Removing data set from solr and mongo");
        LOGGER.info("Data set removed");
    }
}
