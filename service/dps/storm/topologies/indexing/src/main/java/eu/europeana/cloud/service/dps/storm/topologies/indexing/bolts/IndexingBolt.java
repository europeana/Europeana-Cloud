package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingEnvironment;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.utils.IndexingSettingsGenerator;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;

import java.io.Closeable;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pwozniak on 4/6/18
 */
public class IndexingBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingBolt.class);

    private static final String MISSING_INDEXER_POOL_MESSAGE = "IndexerPool is missing. " +
            "Probably You are trying to use alternative environment that is not defined in properties file.";

    private static final int MAX_IDLE_TIME_FOR_INDEXER_IN_SECS = 600;
    private static final int IDLE_TIME_CHECK_INTERVAL_IN_SECS = 60;

    private transient IndexerPoolWrapper indexerPoolWrapper;

    private Properties indexingProperties;

    public IndexingBolt(Properties indexingProperties) {
        this.indexingProperties = indexingProperties;
    }

    @Override
    public void prepare() {
        try {
            indexerPoolWrapper = new IndexerPoolWrapper(MAX_IDLE_TIME_FOR_INDEXER_IN_SECS,
                    IDLE_TIME_CHECK_INTERVAL_IN_SECS);
        } catch (IndexingException | URISyntaxException e) {
            LOGGER.error("Unable to initialize indexer", e);
        }
    }

    @Override
    public void cleanup() {
        if (indexerPoolWrapper != null) {
            indexerPoolWrapper.close();
        }
        super.cleanup();
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {

        // Get variables.
        final String useAltEnv = stormTaskTuple.getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV);
        final String database = stormTaskTuple.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE);
        final String preserveTimestampsString = stormTaskTuple.getParameter(PluginParameterKeys.METIS_PRESERVE_TIMESTAMPS);
        LOGGER.info("Indexing bolt executed for: {} (alternative environment: {}, preserve timestamps: {}).",
                database, useAltEnv, preserveTimestampsString);

        // Obtain indexer pool.
        final IndexerPool indexerPool = indexerPoolWrapper.getIndexerPool(useAltEnv, database);
        if (indexerPool == null) {
            LOGGER.error(MISSING_INDEXER_POOL_MESSAGE);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), MISSING_INDEXER_POOL_MESSAGE, "Error while indexing");
            return;
        }

        // Perform indexing.
        final boolean preserveTimestamps = "true".equalsIgnoreCase(preserveTimestampsString);
        try {
            final String document = new String(stormTaskTuple.getFileData());
            indexerPool.index(document, preserveTimestamps);
            stormTaskTuple.setFileData((byte[]) null);
            outputCollector.emit(stormTaskTuple.toStormTuple());
        } catch (IndexingException e) {
            LOGGER.error("Unable to index file", e);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error while indexing. The full error is: " + ExceptionUtils.getStackTrace(e));
        }
    }

    class IndexerPoolWrapper implements Closeable {

        private IndexerPool indexerPoolForPreviewDbInDefaultEnv;
        private IndexerPool indexerPoolForPublishDbInDefaultEnv;

        private IndexerPool indexerPoolForPreviewDbInAnotherEnv;
        private IndexerPool indexerPoolForPublishDbInAnotherEnv;

        public IndexerPoolWrapper(long maxIdleTimeForIndexerInSecs,
                                  long idleTimeCheckIntervalInSecs) throws IndexingException, URISyntaxException {
            init(maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);
        }

        private void init(long maxIdleTimeForIndexerInSecs, long idleTimeCheckIntervalInSecs)
                throws IndexingException, URISyntaxException {
            IndexingSettingsGenerator settingsGeneratorForDefaultEnv = new IndexingSettingsGenerator(indexingProperties);
            IndexingSettingsGenerator settingsGeneratorForAnotherEnv = new IndexingSettingsGenerator(TargetIndexingEnvironment.ALTERNATIVE, indexingProperties);

            IndexingSettings indexingSettingsForPreviewDbInDefaultEnv = settingsGeneratorForDefaultEnv.generateForPreview();
            IndexingSettings indexingSettingsForPublishDbInDefaultEnv = settingsGeneratorForDefaultEnv.generateForPublish();

            IndexingSettings indexingSettingsForPreviewDbInAnotherEnv = settingsGeneratorForAnotherEnv.generateForPreview();
            IndexingSettings indexingSettingsForPublishDbInAnotherEnv = settingsGeneratorForAnotherEnv.generateForPublish();

            indexerPoolForPreviewDbInDefaultEnv = new IndexerPool(indexingSettingsForPreviewDbInDefaultEnv,
                    maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);
            indexerPoolForPublishDbInDefaultEnv = new IndexerPool(indexingSettingsForPublishDbInDefaultEnv,
                    maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);

            if (indexingSettingsForPreviewDbInAnotherEnv != null) {
                indexerPoolForPreviewDbInAnotherEnv = new IndexerPool(indexingSettingsForPreviewDbInAnotherEnv,
                        maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);
            }
            if (indexingSettingsForPublishDbInAnotherEnv != null) {
                indexerPoolForPublishDbInAnotherEnv = new IndexerPool(indexingSettingsForPublishDbInAnotherEnv,
                        maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);
            }
        }

        IndexerPool getIndexerPool(String altEnv, String database) {
            if (altEnv != null && altEnv.equalsIgnoreCase("true")) {
                if (TargetIndexingDatabase.PREVIEW.toString().equals(database))
                    return indexerPoolForPreviewDbInAnotherEnv;
                else if (TargetIndexingDatabase.PUBLISH.toString().equals(database))
                    return indexerPoolForPublishDbInAnotherEnv;
            } else {
                if (TargetIndexingDatabase.PREVIEW.toString().equals(database))
                    return indexerPoolForPreviewDbInDefaultEnv;
                else if (TargetIndexingDatabase.PUBLISH.toString().equals(database))
                    return indexerPoolForPublishDbInDefaultEnv;
            }
            throw new RuntimeException("Specified environment and/or database is not recognized");
        }

        @Override
        public void close() {
            if (indexerPoolForPreviewDbInDefaultEnv != null) {
                indexerPoolForPreviewDbInDefaultEnv.close();
            }
            if (indexerPoolForPublishDbInDefaultEnv != null) {
                indexerPoolForPublishDbInDefaultEnv.close();
            }
            if (indexerPoolForPreviewDbInAnotherEnv != null) {
                indexerPoolForPreviewDbInAnotherEnv.close();
            }
            if (indexerPoolForPublishDbInAnotherEnv != null) {
                indexerPoolForPreviewDbInAnotherEnv.close();
            }
        }
    }
}
