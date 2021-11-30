package eu.europeana.cloud.service.dps.storm.topologies.properties;

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class PropertyFileLoaderTest {

    String DEFAULT_PROPERTIES_FILE = "test-config.properties";
    String PROVIDED_PROPERTIES_FILE = "src/main/resources/test-config.properties";
    public static Properties topologyProperties;
    PropertyFileLoader reader;

    @Before
    public void init() {
        topologyProperties = new Properties();
        reader = new PropertyFileLoader();
    }

    @Test
    public void testLoadingDefaultPropertiesFile() throws IOException {
        reader.loadDefaultPropertyFile(DEFAULT_PROPERTIES_FILE, topologyProperties);
        assertNotNull(topologyProperties);
        assertFalse(topologyProperties.isEmpty());
        for (final Map.Entry<Object, Object> e : topologyProperties.entrySet()) {
            assertNotNull(e.getKey());
        }

    }

    @Test
    public void testLoadingProvidedPropertiesFile() throws IOException {
        reader.loadProvidedPropertyFile(PROVIDED_PROPERTIES_FILE, topologyProperties);
        assertNotNull(topologyProperties);
        assertFalse(topologyProperties.isEmpty());
        for (final Map.Entry<Object, Object> e : topologyProperties.entrySet()) {
            assertNotNull(e.getKey());

        }
    }

    @Test(expected = FileNotFoundException.class)
    public void testLoadingNonExistedDefaultFile() throws IOException {
        reader.loadDefaultPropertyFile("NON_EXISTED_FILE", topologyProperties);
    }

    @Test(expected = FileNotFoundException.class)
    public void testLoadingNonExistedProvidedFile() throws IOException {
        reader.loadProvidedPropertyFile("NON_EXISTED_FILE", topologyProperties);
    }


    @Test
    public void testLoadingFileWhenProvidedPropertyFileNotExisted() throws IOException {
        PropertyFileLoader.loadPropertyFile(DEFAULT_PROPERTIES_FILE, "NON_EXISTED_PROVIDED_FILE", topologyProperties);
        assertNotNull(topologyProperties);
        assertFalse(topologyProperties.isEmpty());
        for (final Map.Entry<Object, Object> e : topologyProperties.entrySet()) {
            assertNotNull(e.getKey());
        }
    }


    //in case of provided property file and not existing default file. the loaded property file will be empty !
    @Test
    public void testLoadingFileWhenDefaultFileNotExists()
            throws IOException

    {
        PropertyFileLoader.loadPropertyFile("NON_EXISTED_DEFAULT_FILE", PROVIDED_PROPERTIES_FILE, topologyProperties);
        assertTrue(topologyProperties.isEmpty());
    }
}