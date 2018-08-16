package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingEnvironment;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.utils.IndexingSettingsGenerator;
import eu.europeana.indexing.*;
import eu.europeana.indexing.exception.IndexerConfigurationException;
import eu.europeana.indexing.exception.IndexingException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Created by pwozniak on 4/6/18
 */
public class IndexingBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingBolt.class);

    private static final String MISSING_INDEXER_FACTORY_MESSAGE = "IndexerFactory is missing. " +
            "Probably You are trying to use alternative environment that is not defined in properties file.";

    private transient IndexerFactoryWrapper indexerFactoryWrapper;

    private Properties indexingProperties;

    public IndexingBolt(Properties indexingProperties) {
        this.indexingProperties = indexingProperties;
    }

    @Override
    public void prepare() {
        try {
            indexerFactoryWrapper = new IndexerFactoryWrapper();

        } catch (IndexerConfigurationException | URISyntaxException e) {
            LOGGER.error("Unable to initialize indexer", e);
        }
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        String useAltEnv = stormTaskTuple.getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV);
        String database = stormTaskTuple.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE);
        String preserveTimestamps = stormTaskTuple.getParameter(PluginParameterKeys.METIS_PRESERVE_TIMESTAMPS);
        LOGGER.info("Indexing bolt executed for: {} (alternative environment: {}, preserve timestamps: {}).",
            database, useAltEnv, preserveTimestamps);
        IndexerFactory indexerFactory = indexerFactoryWrapper.getIndexerFactory(useAltEnv, database);
        if(indexerFactory == null){
            LOGGER.error(MISSING_INDEXER_FACTORY_MESSAGE);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), MISSING_INDEXER_FACTORY_MESSAGE, "Error while indexing");
            return;
        }
        try (final Indexer indexer = indexerFactory.getIndexer(
            preserveTimestamps != null && preserveTimestamps.equalsIgnoreCase("true"))) {
            String document = new String(stormTaskTuple.getFileData());
            indexer.index(document);
            outputCollector.emit(stormTaskTuple.toStormTuple());
        } catch (IndexerConfigurationException e) {
            LOGGER.error("Unable to index file", e);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error in indexer configuration. The full error is: "+ ExceptionUtils.getStackTrace(e));
        } catch (IOException e) {
            LOGGER.error("Unable to index file", e);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error while retrieving indexer. The full error is: "+ExceptionUtils.getStackTrace(e));
        } catch (IndexingException e) {
            LOGGER.error("Unable to index file", e);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error while indexing. The full error is: "+ExceptionUtils.getStackTrace(e));
        }
    }

    class IndexerFactoryWrapper{

        private IndexerFactory indexerFactoryForPreviewDbInDefaultEnv;
        private IndexerFactory indexerFactoryForPublishDbInDefaultEnv;

        private IndexerFactory indexerFactoryForPreviewDbInAnotherEnv;
        private IndexerFactory indexerFactoryForPublishDbInAnotherEnv;

        public IndexerFactoryWrapper() throws IndexerConfigurationException, URISyntaxException {
            init();
        }

        private void init() throws IndexerConfigurationException, URISyntaxException {
            IndexingSettingsGenerator settingsGeneratorForDefaultEnv = new IndexingSettingsGenerator(indexingProperties);
            IndexingSettingsGenerator settingsGeneratorForAnotherEnv = new IndexingSettingsGenerator(TargetIndexingEnvironment.ALTERNATIVE, indexingProperties);

            IndexingSettings indexingSettingsForPreviewDbInDefaultEnv = settingsGeneratorForDefaultEnv.generateForPreview();
            IndexingSettings indexingSettingsForPublishDbInDefaultEnv = settingsGeneratorForDefaultEnv.generateForPublish();

            IndexingSettings indexingSettingsForPreviewDbInAnotherEnv = settingsGeneratorForAnotherEnv.generateForPreview();
            IndexingSettings indexingSettingsForPublishDbInAnotherEnv = settingsGeneratorForAnotherEnv.generateForPublish();

            indexerFactoryForPreviewDbInDefaultEnv = new IndexerFactory(indexingSettingsForPreviewDbInDefaultEnv);
            indexerFactoryForPublishDbInDefaultEnv = new IndexerFactory(indexingSettingsForPublishDbInDefaultEnv);

            if (indexingSettingsForPreviewDbInAnotherEnv != null) {
                indexerFactoryForPreviewDbInAnotherEnv = new IndexerFactory(indexingSettingsForPreviewDbInAnotherEnv);
            }
            if (indexingSettingsForPublishDbInAnotherEnv != null) {
                indexerFactoryForPublishDbInAnotherEnv = new IndexerFactory(indexingSettingsForPublishDbInAnotherEnv);
            }
        }

        IndexerFactory getIndexerFactory(String altEnv, String database) {
            if (altEnv != null && altEnv.equalsIgnoreCase("true")) {
                if (TargetIndexingDatabase.PREVIEW.toString().equals(database))
                    return indexerFactoryForPreviewDbInAnotherEnv;
                else if (TargetIndexingDatabase.PUBLISH.toString().equals(database))
                    return indexerFactoryForPublishDbInAnotherEnv;
            } else {
                if (TargetIndexingDatabase.PREVIEW.toString().equals(database))
                    return indexerFactoryForPreviewDbInDefaultEnv;
                else if (TargetIndexingDatabase.PUBLISH.toString().equals(database))
                    return indexerFactoryForPublishDbInDefaultEnv;
            }
            throw new RuntimeException("Specified environment and/or database is not recognized");
        }
    }
}
