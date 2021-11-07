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
import java.net.URISyntaxException;
import java.util.EnumMap;
import java.util.Map;

enum DATABASE_LOCATION {
    DEFAULT_PREVIEW,
    DEFAULT_PUBLISH,
    ALT_PREVIEW,
    ALT_PUBLISH,
}

/**
 * Retrieves statistics related with Metis dataset.
 */
public class DatasetStatsRetriever extends IndexWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetStatsRetriever.class);

    private final Map<DATABASE_LOCATION, Indexer> indexers = new EnumMap<>(DATABASE_LOCATION.class);

    public DatasetStatsRetriever() {
        try {
            loadProperties();
            prepareIndexers();
        } catch (IndexingException | URISyntaxException e) {
            LOGGER.error("Unable to load indexers");
            e.printStackTrace();
        }
    }

    public long getTotalRecordsForDataset(DataSetParameters dataSetParameters) throws IndexingException {
        DATABASE_LOCATION databaseLocation = evaluateDatabaseLocation(dataSetParameters);
        return indexers.get(databaseLocation).countRecords(dataSetParameters.getDataSetId());
    }

    private DATABASE_LOCATION evaluateDatabaseLocation(DataSetParameters dataSetParameters) {
        if (dataSetParameters.getTargetIndexingDatabase().equals(TargetIndexingDatabase.PUBLISH)) {
            if (dataSetParameters.getTargetIndexingEnvironment().equals(TargetIndexingEnvironment.ALTERNATIVE)) {
                return DATABASE_LOCATION.ALT_PUBLISH;
            } else {
                return DATABASE_LOCATION.ALT_PREVIEW;
            }
        } else if (dataSetParameters.getTargetIndexingDatabase().equals(TargetIndexingDatabase.PREVIEW)) {
            if (dataSetParameters.getTargetIndexingEnvironment().equals(TargetIndexingEnvironment.DEFAULT)) {
                return DATABASE_LOCATION.DEFAULT_PREVIEW;
            } else {
                return DATABASE_LOCATION.DEFAULT_PUBLISH;
            }
        }
        throw new NullPointerException("Indexer not found");
    }

    private void prepareIndexers() throws IndexingException, URISyntaxException {
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

    @PreDestroy
    private void close() {
        indexers.values().forEach(indexer -> {
            try {
                indexer.close();
            } catch (IOException e) {
                LOGGER.error("Unable to close indexer", e);
                e.printStackTrace();
            }
        });
    }
}