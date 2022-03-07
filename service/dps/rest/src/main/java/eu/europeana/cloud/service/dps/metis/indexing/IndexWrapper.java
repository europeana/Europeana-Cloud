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
    protected final Map<DatabaseLocation, Indexer> indexers = new EnumMap<>(DatabaseLocation.class);

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
        indexers.put(DatabaseLocation.DEFAULT_PREVIEW, new IndexerFactory(indexingSettings).getIndexer());
//        indexingSettings = indexingSettingsGenerator.generateForPublish();
//        indexers.put(DatabaseLocation.DEFAULT_PUBLISH, new IndexerFactory(indexingSettings).getIndexer());
//        //
//        //
//        indexingSettingsGenerator = new IndexingSettingsGenerator(TargetIndexingEnvironment.ALTERNATIVE, properties);
//
//        indexingSettings = indexingSettingsGenerator.generateForPreview();
//        if(indexingSettings != null) {
//            indexers.put(DatabaseLocation.ALT_PREVIEW, new IndexerFactory(indexingSettings).getIndexer());
//        }
//        indexingSettings = indexingSettingsGenerator.generateForPublish();
//        if(indexingSettings != null) {
//            indexers.put(DatabaseLocation.ALT_PUBLISH, new IndexerFactory(indexingSettings).getIndexer());
//        }
    }

    protected DatabaseLocation evaluateDatabaseLocation(MetisDataSetParameters metisDataSetParameters) {
        if (metisDataSetParameters.getTargetIndexingDatabase().equals(TargetIndexingDatabase.PUBLISH)) {
            if (metisDataSetParameters.getTargetIndexingEnvironment().equals(TargetIndexingEnvironment.ALTERNATIVE)) {
                return DatabaseLocation.ALT_PUBLISH;
            } else {
                return DatabaseLocation.DEFAULT_PUBLISH;
            }
        } else if (metisDataSetParameters.getTargetIndexingDatabase().equals(TargetIndexingDatabase.PREVIEW)) {
            if (metisDataSetParameters.getTargetIndexingEnvironment().equals(TargetIndexingEnvironment.ALTERNATIVE)) {
                return DatabaseLocation.ALT_PREVIEW;
            } else {
                return DatabaseLocation.DEFAULT_PREVIEW;
            }
        }
        throw new NullPointerException("Indexer not found");
    }

    @PreDestroy
    public void close() {
        indexers.values().forEach(indexer -> {
            try {
                indexer.close();
            } catch (IOException e) {
                LOGGER.error("Unable to close indexer", e);
            }
        });
    }
}
