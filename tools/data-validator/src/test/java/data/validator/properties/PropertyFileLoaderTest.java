package data.validator.properties;

import data.validator.properities.PropertyFileLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by Tarek on 11/13/2015.
 */
public class PropertyFileLoaderTest {

  String DEFAULT_PROPERTIES_FILE = "test.properties";
  String PROVIDED_PROPERTIES_FILE = "src/main/resources/test.properties";
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

  }

  @Test
  public void testLoadingProvidedPropertiesFile() throws IOException {
    reader.loadProvidedPropertyFile(PROVIDED_PROPERTIES_FILE, topologyProperties);
    assertNotNull(topologyProperties);
    assertFalse(topologyProperties.isEmpty());

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

  }

}