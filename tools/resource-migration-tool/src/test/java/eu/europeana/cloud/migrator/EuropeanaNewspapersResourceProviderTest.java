package eu.europeana.cloud.migrator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import eu.europeana.cloud.migrator.provider.EuropeanaNewspapersResourceProvider;
import eu.europeana.cloud.migrator.provider.FilePaths;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class EuropeanaNewspapersResourceProviderTest {

  private static final String REPRESENTATION_NAME = "presentation_images";

  private static final String MAPPING_FILE_NAME = "src/test/resources/mapping.csv";

  private static final String LOCAL_LOCATIONS = "file:///$1/test1;file:///$1/test2";

  private static final String REMOTE_LOCATIONS = "http://www.test.com/test1;http://www.test.com/test2";

  private static final String MIXED_LOCATIONS = "http://www.test.com/test1;file:///c:/temp/test2";

  private static final String LOCATION_1 = "file:///$1/test1";

  private static final String LOCATION_2 = "file:///$1/test2";

  private static final String LOCAL_ID_1 = "1234";

  private static final String LOCAL_ID_2 = "9293";

  private static final String PATH_1 = "node-1/image/LFT/Alpenzeitung/1926/04/02";

  private static final String PATH_2 = "node-2/image/NLE/Eesti_Postimees/1864/02/05/1";

  private static final String PATH_NON_EXISTING = "node-3/image/SUBHH/Berliner/1945/01/01";

  private static final String FILE_1 = "AZ_1926_04_02_0001.jp2";

  private static final String FILE_2 = "18640205_1-0001.jp2";

  private static final String FILE_3 = "AZ1_1926_04_02_0001.jp2";

  private static final String FILE_4 = "AZ1_1926_04_02_0001.jp2";

  private static final String PROVIDER_1 = "LFT";

  private static final String PROVIDER_2 = "NLE";

  private static final String DATA_PROVIDER = "TheEuropeanLibrary";
  private EuropeanaNewspapersResourceProvider provider;

  private String resDir;


  @Before
  public void setUp() throws Exception {
    resDir = Paths.get(Paths.get(".").toAbsolutePath().normalize().toString(), "src/test/resources").toAbsolutePath().normalize()
                  .toString().replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
  }


  @Test
  public void testGetResourceProviderId() throws Exception {
    provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME,
        LOCAL_LOCATIONS.replace("$1", resDir), DATA_PROVIDER);

    String file1 = Paths.get(new URI(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_1)).toAbsolutePath().toString()
                        .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertEquals(PROVIDER_1, provider.getResourceProviderId(file1));
    String file2 = Paths.get(new URI(LOCATION_2.replace("$1", resDir) + "/" + PATH_2 + "/" + FILE_2)).toAbsolutePath().toString()
                        .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertEquals(PROVIDER_2, provider.getResourceProviderId(file2));
  }


  @Test
  public void testGetDataProviderId() throws Exception {
    provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME,
        LOCAL_LOCATIONS.replace("$1", resDir), DATA_PROVIDER);

    // data provider identifier should be independent from the path, always the same and the one provided in constructor
    String file1 = Paths.get(new URI(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_1)).toAbsolutePath().toString()
                        .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertEquals(DATA_PROVIDER, provider.getDataProviderId(file1));
    String file2 = Paths.get(new URI(LOCATION_2.replace("$1", resDir) + "/" + PATH_2 + "/" + FILE_2)).toAbsolutePath().toString()
                        .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertEquals(DATA_PROVIDER, provider.getDataProviderId(file2));
  }


  @Test
  public void testGetLocalIdentifier() throws Exception {
    provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME,
        LOCAL_LOCATIONS.replace("$1", resDir), DATA_PROVIDER);

    String file = Paths.get(new URI(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_1)).toAbsolutePath().toString()
                       .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    String localId = provider.getLocalIdentifier(Paths.get(new URI(LOCATION_1.replace("$1", resDir))).toAbsolutePath().toString()
                                                      .replace(ResourceMigrator.WINDOWS_SEPARATOR,
                                                          ResourceMigrator.LINUX_SEPARATOR), file, false);
    assertEquals(localId, LOCAL_ID_1);
    file = Paths.get(new URI(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_3)).toAbsolutePath().toString()
                .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    localId = provider.getLocalIdentifier(Paths.get(new URI(LOCATION_1.replace("$1", resDir))).toAbsolutePath().toString()
                                               .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR),
        file, false);
    assertEquals(localId, LOCAL_ID_2);
    file = Paths.get(new URI(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_4)).toAbsolutePath().toString()
                .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    localId = provider.getLocalIdentifier(Paths.get(new URI(LOCATION_1.replace("$1", resDir))).toAbsolutePath().toString()
                                               .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR),
        file, false);
    assertEquals(localId, LOCAL_ID_2);
  }

  @Test
  public void testGetLocalIdentifierReturnsNull() throws Exception {
    provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME,
        LOCAL_LOCATIONS.replace("$1", resDir), DATA_PROVIDER);

    // location is different
    String file1 = Paths.get(new URI(LOCATION_2.replace("$1", resDir) + "/" + PATH_2 + "/" + FILE_2)).toAbsolutePath().toString()
                        .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    String localId = provider.getLocalIdentifier(LOCATION_1.replace("$1", resDir), file1, false);
    assertNull(localId);

    // no association between path and local identifier
    file1 = Paths.get(new URI(LOCATION_1.replace("$1", resDir) + "/" + PATH_NON_EXISTING + "/" + FILE_1)).toAbsolutePath()
                 .toString().replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    localId = provider.getLocalIdentifier(LOCATION_1.replace("$1", resDir), file1, false);
    assertNull(localId);
  }

  @Test
  public void testScan() throws Exception {
    provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME,
        LOCAL_LOCATIONS.replace("$1", resDir), DATA_PROVIDER);

    Map<String, List<FilePaths>> paths = provider.scan();

    // there should be 2 data providers
    assertEquals(2, paths.size());
    // there should be provider LFT
    assertNotNull(paths.get(PROVIDER_1));
    // there should be provider NLE
    assertNotNull(paths.get(PROVIDER_2));
    // each provider should have one FilePaths object
    assertEquals(1, paths.get(PROVIDER_1).size());
    assertEquals(1, paths.get(PROVIDER_2).size());
    // FilePaths object should have proper location and provider
    String path = Paths.get(new URI(LOCATION_1.replace("$1", resDir))).toAbsolutePath().toString()
                       .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertEquals(path, paths.get(PROVIDER_1).get(0).getLocation());
    path = Paths.get(new URI(LOCATION_2.replace("$1", resDir))).toAbsolutePath().toString()
                .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertEquals(path, paths.get(PROVIDER_2).get(0).getLocation());
    assertEquals(PROVIDER_1, paths.get(PROVIDER_1).get(0).getDataProvider());
    assertEquals(PROVIDER_2, paths.get(PROVIDER_2).get(0).getDataProvider());
    // there should be only one path in FilePaths object
    assertEquals(3, paths.get(PROVIDER_1).get(0).size());
    assertEquals(1, paths.get(PROVIDER_2).get(0).size());
    // path should be equal to the real one

    List<String> allPaths = getAllPaths(paths.get(PROVIDER_1).get(0));

    path = Paths.get(new URI(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_1)).toAbsolutePath().toString()
                .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertTrue(allPaths.contains(path));
    path = Paths.get(new URI(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_3)).toAbsolutePath().toString()
                .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertTrue(allPaths.contains(path));
    path = Paths.get(new URI(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_4)).toAbsolutePath().toString()
                .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertTrue(allPaths.contains(path));

    allPaths = getAllPaths(paths.get(PROVIDER_2).get(0));

    path = Paths.get(new URI(LOCATION_2.replace("$1", resDir) + "/" + PATH_2 + "/" + FILE_2)).toAbsolutePath().toString()
                .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    assertTrue(allPaths.contains(path));
  }

  private List<String> getAllPaths(FilePaths paths) {
    BufferedReader reader = paths.getPathsReader();
    assertNotNull(reader);
    List<String> allPaths = new ArrayList<String>();
    try {
      for (; ; ) {
        String line = null;
        line = reader.readLine();
        if (line == null) {
          break;
        }
        allPaths.add(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return allPaths;
  }

  @Test
  public void testDetectLocalLocations() throws Exception {
    provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME,
        LOCAL_LOCATIONS.replace("$1", resDir), DATA_PROVIDER);

    assertTrue(provider.isLocal());
  }


  @Test
  public void testDetectRemoteLocations() throws Exception {
    provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, REMOTE_LOCATIONS, DATA_PROVIDER);

    assertFalse(provider.isLocal());
  }


  @Test(expected = IllegalArgumentException.class)
  public void testDetectMixedLocations() throws Exception {
    // just create a new object, locations are detected in constructor
    provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, MIXED_LOCATIONS, DATA_PROVIDER);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testNullDataProvider() throws Exception {
    // just create a new object, data provider is checked in constructor
    provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, LOCAL_LOCATIONS, null);
  }
}