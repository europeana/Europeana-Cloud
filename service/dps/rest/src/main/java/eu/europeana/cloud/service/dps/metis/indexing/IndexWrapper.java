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
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;

/**
 * Wraps operations on the index
 */
public abstract class IndexWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexWrapper.class);
    protected final Properties properties = new Properties();
    protected final Map<DATABASE_LOCATION, Indexer> indexers = new EnumMap<>(DATABASE_LOCATION.class);

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
            InputStream input = DatasetCleaner.class.getClassLoader().getResourceAsStream("indexing.properties");
            properties.load(input);
        } catch (Exception e) {
            LOGGER.warn("Unable to read indexing.properties (are you sure that file exists?). Dataset will not  be cleared before indexing.");
        }
    }

    protected void prepareIndexers() throws IndexingException, URISyntaxException {
        IndexingSettingsGenerator indexingSettingsGenerator = new IndexingSettingsGenerator(properties);

        IndexingSettings indexingSettings = indexingSettingsGenerator.generateForPreview();
        indexers.put(DATABASE_LOCATION.DEFAULT_PREVIEW, new IndexerFactory(indexingSettings).getIndexer());
        indexingSettings = indexingSettingsGenerator.generateForPublish();
        indexers.put(DATABASE_LOCATION.DEFAULT_PUBLISH, new IndexerFactory(indexingSettings).getIndexer());
        //
        indexingSettingsGenerator = new IndexingSettingsGenerator(TargetIndexingEnvironment.ALTERNATIVE, properties);
        indexingSettings = indexingSettingsGenerator.generateForPreview();
        indexers.put(DATABASE_LOCATION.ALT_PREVIEW, new IndexerFactory(indexingSettings).getIndexer());
        indexingSettings = indexingSettingsGenerator.generateForPublish();
        indexers.put(DATABASE_LOCATION.ALT_PUBLISH, new IndexerFactory(indexingSettings).getIndexer());
    }

    protected DATABASE_LOCATION evaluateDatabaseLocation(MetisDataSetParameters metisDataSetParameters) {
        if (metisDataSetParameters.getTargetIndexingDatabase().equals(TargetIndexingDatabase.PUBLISH)) {
            if (metisDataSetParameters.getTargetIndexingEnvironment().equals(TargetIndexingEnvironment.ALTERNATIVE)) {
                return DATABASE_LOCATION.ALT_PUBLISH;
            } else {
                return DATABASE_LOCATION.DEFAULT_PUBLISH;
            }
        } else if (metisDataSetParameters.getTargetIndexingDatabase().equals(TargetIndexingDatabase.PREVIEW)) {
            if (metisDataSetParameters.getTargetIndexingEnvironment().equals(TargetIndexingEnvironment.ALTERNATIVE)) {
                return DATABASE_LOCATION.ALT_PREVIEW;
            } else {
                return DATABASE_LOCATION.DEFAULT_PREVIEW;
            }
        }
        throw new NullPointerException("Indexer not found");
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
