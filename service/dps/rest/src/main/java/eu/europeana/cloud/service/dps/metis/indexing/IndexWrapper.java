package eu.europeana.cloud.service.dps.metis.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Wraps operations on the index
 */
public abstract class IndexWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexWrapper.class);
    protected final Properties properties = new Properties();

    protected void loadProperties() {
        try {
            InputStream input = DatasetCleaner.class.getClassLoader().getResourceAsStream("indexing.properties");
            properties.load(input);
        } catch (Exception e) {
            LOGGER.warn("Unable to read indexing.properties (are you sure that file exists?). Dataset will not  be cleared before indexing.");
        }
    }

}
