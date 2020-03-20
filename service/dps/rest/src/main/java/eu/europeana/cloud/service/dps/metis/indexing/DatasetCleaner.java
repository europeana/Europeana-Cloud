package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.cloud.service.dps.service.utils.indexing.IndexingSettingsGenerator;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingEnvironment;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.ParseException;
import java.util.Properties;

/**
 * Remove dataset based on a specific date for indexing topology.
 * <p>
 * Created by pwozniak on 10/2/18
 */
public class DatasetCleaner {

    private final static Logger LOGGER = LoggerFactory.getLogger(DatasetCleaner.class);
    private IndexerFactory indexerFactory;
    private Properties properties = new Properties();

    private DataSetCleanerParameters cleanerParameters;

    public DatasetCleaner(DataSetCleanerParameters cleanerParameters) {
        this.cleanerParameters = cleanerParameters;
        loadProperties();
    }

    public void execute() throws DatasetCleaningException, ParseException {
        LOGGER.info("Executing initial actions for indexing topology");
        if (properties.isEmpty()) {
            return;
        }
        prepareIndexerFactory();
        try {
            removeDataSet(cleanerParameters.getDataSetId());
        } catch (IndexingException e) {
            LOGGER.error("Dataset was not removed correctly. ", e);
            throw new DatasetCleaningException("Dataset was not removed correctly.", e);
        }
    }

    private void loadProperties() {
        try {
            InputStream input = DatasetCleaner.class.getClassLoader().getResourceAsStream("indexing.properties");
            properties.load(input);
        } catch (Exception e) {
            LOGGER.warn("Unable to read indexing.properties (are you sure that file exists?). Dataset will not  be cleared before indexing.");
        }
    }

    private void prepareIndexerFactory() {
        LOGGER.debug("Preparing IndexerFactory for removing datasets from Solr and Mongo");
        //
        boolean altEnv = cleanerParameters.isUsingAltEnv();
        final String targetIndexingEnv = cleanerParameters.getTargetIndexingEnv();
        //
        IndexingSettings indexingSettings = null;
        try {
            if (true == altEnv) {
                IndexingSettingsGenerator s1 = new IndexingSettingsGenerator(TargetIndexingEnvironment.ALTERNATIVE, properties);
                if (TargetIndexingDatabase.PREVIEW.toString().equals(targetIndexingEnv))
                    indexingSettings = s1.generateForPreview();
                else if (TargetIndexingDatabase.PUBLISH.toString().equals(targetIndexingEnv))
                    indexingSettings = s1.generateForPublish();
            } else {
                IndexingSettingsGenerator s2 = new IndexingSettingsGenerator(properties);
                if (TargetIndexingDatabase.PREVIEW.toString().equals(targetIndexingEnv))
                    indexingSettings = s2.generateForPreview();
                else if (TargetIndexingDatabase.PUBLISH.toString().equals(targetIndexingEnv))
                    indexingSettings = s2.generateForPublish();
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to create indexing factory");
        }
        indexerFactory = new IndexerFactory(indexingSettings);
    }

    private void removeDataSet(String datasetId) throws IndexingException, ParseException {
        LOGGER.info("Removing data set {} from solr and mongo", datasetId);
        indexerFactory.getIndexer().removeAll(datasetId, cleanerParameters.getCleaningDate());
        LOGGER.info("Data set removed");
    }
}