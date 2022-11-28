package eu.europeana.cloud.migrator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.migrator.provider.DefaultResourceProvider;
import eu.europeana.cloud.migrator.provider.FoodAndDrinkResourceProvider;
import java.net.URI;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;

public class FoodAndDrinkResourceProviderTest {

  private static final String REPRESENTATION_NAME = "presentation";

  private static final String LOCAL_LOCATIONS = "file:///$1/test3";

  private static final String LOCATION = "file:///$1/test3";

  private static final String LOCAL_ID = "00008270_1.JPG";

  private static final String PATH = "users/DianthaOs";

  private static final String PATH_NON_EXISTING = "users/rmca";

  private static final String FILE = "00008270_1.JPG";

  private static final String PROVIDER = "DianthaOs";

  private static final String ORGANISATION_NAME = "Diantha";

  private static final String OFFICIAL_ADDRESS = "Diantha street";

  private static final String ORGANISATION_WEBSITE = "Diantha www";

  private static final String ORGANISATION_WEBSITE_URL = "www.diantha.com";

  private static final String DIGITAL_LIBRARY_WEBSITE = "Diantha www";

  private static final String DIGITAL_LIBRARY_URL = "www.diantha.com";

  private static final String CONTACT_PERSON = "Diantha";

  private static final String REMARKS = "Diantha";

  private static final String DP_PROPERTIES_FILE = "DianthaOs.properties";

  private FoodAndDrinkResourceProvider provider;

  private String resDir;

  @Before
  public void setUp() throws Exception {
    resDir = Paths.get(Paths.get(".").toAbsolutePath().normalize().toString(), "src/test/resources").toAbsolutePath().normalize()
                  .toString().replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    provider = new FoodAndDrinkResourceProvider(REPRESENTATION_NAME, null, LOCAL_LOCATIONS.replace("$1", resDir), null);
  }

  @Test
  public void testGetLocalIdentifier() throws Exception {
    String file1 = Paths.get(new URI(LOCATION.replace("$1", resDir) + "/" + PATH + "/" + FILE)).toAbsolutePath().toString()
                        .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    String localId = provider.getLocalIdentifier(Paths.get(new URI(LOCATION.replace("$1", resDir))).toAbsolutePath().toString()
                                                      .replace(ResourceMigrator.WINDOWS_SEPARATOR,
                                                          ResourceMigrator.LINUX_SEPARATOR), file1, false);
    assertEquals(localId, LOCAL_ID);
  }

  @Test
  public void testGetProviderId() throws Exception {
    String file = Paths.get(new URI(LOCATION.replace("$1", resDir) + "/" + PATH + "/" + FILE)).toAbsolutePath().toString()
                       .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    String dataProvider = provider.getResourceProviderId(file);
    assertEquals(PROVIDER, dataProvider);
  }

  @Test
  public void testGetDataProviderPropertiesFromPath() throws Exception {
    String file = Paths.get(new URI(LOCATION.replace("$1", resDir) + "/" + PATH)).toAbsolutePath().toString()
                       .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    DataProviderProperties p = provider.getDataProviderProperties(file);
    assertNotNull(p);
    // test whether all props are from file
    assertEquals(ORGANISATION_NAME, p.getOrganisationName());
    assertEquals(OFFICIAL_ADDRESS, p.getOfficialAddress());
    assertEquals(ORGANISATION_WEBSITE, p.getOrganisationWebsite());
    assertEquals(ORGANISATION_WEBSITE_URL, p.getOrganisationWebsiteURL());
    assertEquals(DIGITAL_LIBRARY_WEBSITE, p.getDigitalLibraryWebsite());
    assertEquals(DIGITAL_LIBRARY_URL, p.getDigitalLibraryURL());
    assertEquals(CONTACT_PERSON, p.getContactPerson());
    assertEquals(REMARKS, p.getRemarks());
  }

  @Test
  public void testGetDataProviderPropertiesFromFile() throws Exception {
    String file = Paths.get(new URI(LOCATION.replace("$1", resDir) + "/" + PATH + "/" + DP_PROPERTIES_FILE)).toAbsolutePath()
                       .toString().replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    DataProviderProperties p = provider.getDataProviderProperties(file);
    assertNotNull(p);
    // test whether all props are from file
    assertEquals(ORGANISATION_NAME, p.getOrganisationName());
    assertEquals(OFFICIAL_ADDRESS, p.getOfficialAddress());
    assertEquals(ORGANISATION_WEBSITE, p.getOrganisationWebsite());
    assertEquals(ORGANISATION_WEBSITE_URL, p.getOrganisationWebsiteURL());
    assertEquals(DIGITAL_LIBRARY_WEBSITE, p.getDigitalLibraryWebsite());
    assertEquals(DIGITAL_LIBRARY_URL, p.getDigitalLibraryURL());
    assertEquals(CONTACT_PERSON, p.getContactPerson());
    assertEquals(REMARKS, p.getRemarks());
  }

  @Test
  public void testGetDefaultDataProviderProperties() throws Exception {
    String file = Paths.get(new URI(LOCATION.replace("$1", resDir) + "/" + PATH_NON_EXISTING + "/" + FILE)).toAbsolutePath()
                       .toString().replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    DataProviderProperties p = provider.getDataProviderProperties(file);
    // when no properties file is supplied default values should be used
    assertNotNull(p);
    // test whether all props are default
    assertEquals(DefaultResourceProvider.DEFAULT_ORGANISATION_NAME, p.getOrganisationName());
    assertEquals(DefaultResourceProvider.DEFAULT_OFFICIAL_ADDRESS, p.getOfficialAddress());
    assertEquals(DefaultResourceProvider.DEFAULT_ORGANISATION_WEBSITE, p.getOrganisationWebsite());
    assertEquals(DefaultResourceProvider.DEFAULT_ORGANISATION_WEBSITE_URL, p.getOrganisationWebsiteURL());
    assertEquals(DefaultResourceProvider.DEFAULT_DIGITAL_LIBRARY_WEBSITE, p.getDigitalLibraryWebsite());
    assertEquals(DefaultResourceProvider.DEFAULT_DIGITAL_LIBRARY_WEBSITE_URL, p.getDigitalLibraryURL());
    assertEquals(DefaultResourceProvider.DEFAULT_CONTACT_PERSON, p.getContactPerson());
    assertEquals(DefaultResourceProvider.DEFAULT_REMARKS, p.getRemarks());
  }

  @Test
  public void testGetFilename() throws Exception {
    String file = Paths.get(new URI(LOCATION.replace("$1", resDir) + "/" + PATH + "/" + FILE)).toAbsolutePath().toString()
                       .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR);
    String fileName = provider.getFilename(Paths.get(new URI(LOCATION.replace("$1", resDir))).toAbsolutePath().toString()
                                                .replace(ResourceMigrator.WINDOWS_SEPARATOR, ResourceMigrator.LINUX_SEPARATOR),
        file);
    // file name for ecloud should contain the provider id and the file name itself
    assertEquals(PROVIDER + "/" + FILE, fileName);
  }
}