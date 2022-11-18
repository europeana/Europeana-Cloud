package eu.europeana.cloud.service.dps.service.utils.indexing;

import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
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
public class IndexWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexWrapper.class);
    private static IndexWrapper instance;
    protected final Properties properties;
    protected final Map<TargetIndexingDatabase, Indexer> indexers = new EnumMap<>(TargetIndexingDatabase.class);

    public static synchronized IndexWrapper getInstance(Properties properties) {
        if (instance == null) {
            instance = new IndexWrapper(properties);
        }
        return instance;
    }

    public IndexWrapper(Properties properties) {
        this.properties = properties;
        try {
            prepareIndexers();
        } catch (IndexingException | URISyntaxException e) {
            throw new IndexWrapperException("Unable to load indexers", e);
        }
    }

    public IndexWrapper() {
        this(loadProperties());
    }

    private static Properties loadProperties() {
        try {
            Properties properties=new Properties();
            InputStream input = IndexWrapper.class.getClassLoader().getResourceAsStream(IndexingSettingsGenerator.DEFAULT_PROPERTIES_FILENAME);
            properties.load(input);
            return properties;
        } catch (Exception e) {
            throw new IndexWrapperException("Unable to read indexing.properties (are you sure that file exists?)." +
                    " Dataset will not  be cleared before indexing.",e);
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

    public Indexer getIndexer(TargetIndexingDatabase targetIndexingDatabase) {
        return indexers.get(targetIndexingDatabase);
    }
}
