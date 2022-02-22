package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.cloud.service.dps.service.utils.indexing.IndexingSettingsGenerator;

import java.io.FileInputStream;
import java.io.InputStream;

public class TestIndexWrapper extends IndexWrapper{

    private static final String INDEXING_PROPERTIES_FILE_NAME = "indexing.properties";

    protected void loadProperties() {
        try {
            InputStream input = new FileInputStream(INDEXING_PROPERTIES_FILE_NAME);
            properties.load(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
