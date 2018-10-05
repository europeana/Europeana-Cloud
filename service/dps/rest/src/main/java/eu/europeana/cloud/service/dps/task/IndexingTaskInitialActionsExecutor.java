package eu.europeana.cloud.service.dps.task;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingEnvironment;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexingSettingsGenerator;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * Executes all preliminary jobs related with tasks submitted for indexing topology.
 * <p>
 * Created by pwozniak on 10/2/18
 */
public class IndexingTaskInitialActionsExecutor implements TaskInitialActionsExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTaskInitialActionsExecutor.class);

    private DpsTask dpsTask;
    private String topologyName;
    private IndexerFactory indexerFactory;
    private CassandraTaskInfoDAO taskDAO;

    private Properties properties = new Properties();

    public IndexingTaskInitialActionsExecutor(DpsTask task, String topologyName, CassandraTaskInfoDAO taskDAO) {
        this.dpsTask = task;
        this.topologyName = topologyName;
        this.taskDAO = taskDAO;
        loadProperties();
    }

    @Override
    public void execute() throws InitialActionException {
        LOGGER.info("Executing initial actions for indexing topology");
        changeTaskStatus();
        prepareIndexerFactory();
        try {
            removeDataSet(dpsTask.getParameter(PluginParameterKeys.METIS_DATASET_ID));
        } catch (IndexingException e) {
            LOGGER.error("Dataset was not removed correctly. ", e);
            throw new InitialActionException("Dataset was not removed correctly.", e);
        }
    }

    private void changeTaskStatus() {
        taskDAO.insert(dpsTask.getTaskId(), topologyName, 0, TaskState.BEING_REMOVED.toString(), "The task is in a pending mode, it is being removed from Solr/Mongo before submission", new Date());
    }

    private void loadProperties() {
        try {
            InputStream input = IndexingTaskInitialActionsExecutor.class.getClassLoader().getResourceAsStream("indexing.properties");
            properties.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void prepareIndexerFactory() {
        LOGGER.debug("Preparing IndexerFactory for removing datasets from Solr and Mongo");
        //
        final String altEnv = dpsTask.getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV);
        final String database = dpsTask.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE);
        //
        IndexingSettings indexingSettings = null;
        try {
            if (altEnv != null && altEnv.equalsIgnoreCase("true")) {
                IndexingSettingsGenerator s1 = new IndexingSettingsGenerator(TargetIndexingEnvironment.ALTERNATIVE, properties);
                if (TargetIndexingDatabase.PREVIEW.toString().equals(database))
                    indexingSettings = s1.generateForPreview();
                else if (TargetIndexingDatabase.PUBLISH.toString().equals(database))
                    indexingSettings = s1.generateForPublish();
            } else {
                IndexingSettingsGenerator s2 = new IndexingSettingsGenerator(properties);
                if (TargetIndexingDatabase.PREVIEW.toString().equals(database))
                    indexingSettings = s2.generateForPreview();
                else if (TargetIndexingDatabase.PUBLISH.toString().equals(database))
                    indexingSettings = s2.generateForPublish();
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to create indexing factory");
        }
        indexerFactory = new IndexerFactory(indexingSettings);
    }

    private void removeDataSet(String datasetId) throws IndexingException {
        LOGGER.info("Removing data set {} from solr and mongo", datasetId);
        indexerFactory.getIndexer().removeAll(datasetId);
        LOGGER.info("Data set removed");
    }
}
