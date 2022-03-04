package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.cloud.service.dps.service.utils.indexing.IndexingSettingsGenerator;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Wraps operations on the index
 */
public abstract class IndexWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexWrapper.class);
    protected final Properties properties = new Properties();
    protected final Map<TargetIndexingDatabase, Indexer> indexers = new EnumMap<>(TargetIndexingDatabase.class);

    protected IndexWrapper() {
        try {
            loadProperties();
            prepareIndexers();
        } catch (IndexingException | URISyntaxException e) {
            LOGGER.error("Unable to load indexers", e);
        }
    }

    protected void loadProperties() {
        try {
            InputStream input = DatasetCleaner.class.getClassLoader().getResourceAsStream(IndexingSettingsGenerator.DEFAULT_PROPERTIES_FILENAME);
            properties.load(input);
        } catch (Exception e) {
            LOGGER.warn("Unable to read indexing.properties (are you sure that file exists?). Dataset will not  be cleared before indexing.");
        }
    }

    protected void prepareIndexers() throws IndexingException, URISyntaxException {
        IndexingSettings indexingSettings;
        IndexingSettingsGenerator indexingSettingsGenerator;

        indexingSettingsGenerator = new IndexingSettingsGenerator(properties);

        indexingSettings = indexingSettingsGenerator.generateForPreview();
        indexers.put(TargetIndexingDatabase.PREVIEW, new IndexerFactory(indexingSettings).getIndexer());
        indexingSettings = indexingSettingsGenerator.generateForPublish();
        indexers.put(TargetIndexingDatabase.PUBLISH, new IndexerFactory(indexingSettings).getIndexer());
    }

    @PreDestroy
    private void close() {
        indexers.values().forEach(indexer -> {
            try {
                indexer.close();
            } catch (IOException e) {
                LOGGER.error("Unable to close indexer", e);
            }
        });
    }
}
