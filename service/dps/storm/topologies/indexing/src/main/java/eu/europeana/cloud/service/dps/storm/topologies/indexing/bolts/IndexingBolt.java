package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.utils.IndexingSettingsGenerator;
import eu.europeana.indexing.*;
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

    private transient IndexerFactory indexerFactoryForPreviewEnv;
    private transient IndexerFactory indexerFactoryForPublishEnv;
    private Properties indexingProperties;

    public IndexingBolt(Properties indexingProperties) {
        this.indexingProperties = indexingProperties;
    }

    @Override
    public void prepare() {
        try {
            IndexingSettingsGenerator settingsGenerator = new IndexingSettingsGenerator();
            IndexingSettings indexingSettingsForPreviewEnv = settingsGenerator.generateForPreview(indexingProperties);
            IndexingSettings indexingSettingsForPublishEnv = settingsGenerator.generateForPublish(indexingProperties);
            indexerFactoryForPreviewEnv = new IndexerFactory(indexingSettingsForPreviewEnv);
            indexerFactoryForPublishEnv = new IndexerFactory(indexingSettingsForPublishEnv);
        } catch (IndexerConfigurationException | URISyntaxException e) {
            LOGGER.error("Unable to initialize indexer", e);
        }
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        String environment = stormTaskTuple.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE);
        LOGGER.info("Indexing bolt executed for: {}", environment);
        IndexerFactory indexerFactory = getIndexerFor(environment);
        try (final Indexer indexer = indexerFactory.getIndexer()) {
            String document = new String(stormTaskTuple.getFileData());
            indexer.index(document);
            outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
        } catch (IndexerConfigurationException e) {
            LOGGER.error("Unable to index file", e);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error in indexer configuration");
        } catch (IOException e) {
            LOGGER.error("Unable to index file", e);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error while retrieving indexer");
        } catch (IndexingException e) {
            LOGGER.error("Unable to index file", e);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error while indexing");
        }
    }

    private IndexerFactory getIndexerFor(String environment) {
        if (TargetIndexingDatabase.PREVIEW.toString().equals(environment))
            return indexerFactoryForPreviewEnv;
        else if (TargetIndexingDatabase.PUBLISH.toString().equals(environment))
            return indexerFactoryForPublishEnv;
        else
            throw new RuntimeException("Specified environment is not recognized");
    }
}
