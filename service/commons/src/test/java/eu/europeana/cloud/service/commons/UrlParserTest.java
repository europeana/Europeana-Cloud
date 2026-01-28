package eu.europeana.cloud.service.commons;

import eu.europeana.cloud.service.commons.urls.UrlBuilderException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.MalformedURLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class UrlParserTest {


    private static final String MALFORMED_URL = "fdafsdfasdfadagadfa";
    private static final String RANDOM_URL = "http://blog.europeana.eu/2015/10/10-terrifying-costume-ideas-for-halloween/";
    private static final String URL_WITH_MISPLACED_PARTS = "http://www.example.com/representations/REPRESENTATIONNAME/files/FILENAME/records/CLOUDID/versions/VERSION";
    private static final String URL_TO_FILE = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt";
    private static final String URL_TO_FILES_LIST = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files";
    private static final String URL_TO_VERSIONS_LIST = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions";
    private static final String URL_TO_VERSION = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef";
    private static final String URL_TO_REPRESENTATIONS_LIST = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations";
    private static final String URL_TO_REPRESENTATION = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF";
    private static final String URL_TO_CLOUDID = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ";
    private static final String URL_TO_DATASETS_LIST = "http://127.0.0.1:8080/mcs/data-providers/sampleDP/data-sets";
    private static final String URL_TO_DATASET = "http://127.0.0.1:8080/mcs/data-providers/sampleDP/data-sets/sampleDataSet";

    private static final String TEST = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/permissions/all/users/pwoz";

    @Test
    void shouldThrowMalformedURLException() {
        assertThrows(MalformedURLException.class, () -> {
            UrlParser urlParser = new UrlParser(MALFORMED_URL);
            urlParser.isUrlToRepresentations();
        });
    }

    @ParameterizedTest
    @CsvSource({
            "/folder1/folder2/filename1",
            "/folder1/folder2//filename1",
            "/./folder1/folder2//filename1"
    })
    void shouldParseProperlyUrlToFilePath(final String suffixFilePath) throws MalformedURLException {
        //given
        //when
        UrlParser parser = new UrlParser(URL_TO_FILES_LIST + suffixFilePath);
        //then
        System.out.println(suffixFilePath);
        assertProperFileUrl(parser);
        assertThat(parser.getPart(UrlPart.FILES), is(suffixFilePath));
    }

  private void assertProperFileUrl(UrlParser parser) {
    assertThat(parser.isUrlToRepresentationVersionFile(), is(true));
    assertThat(parser.getPart(UrlPart.VERSIONS), notNullValue());
    assertThat(parser.getPart(UrlPart.REPRESENTATIONS), notNullValue());
    assertThat(parser.getPart(UrlPart.RECORDS), notNullValue());
    assertThat(parser.getPart(UrlPart.FILES), notNullValue());
  }

    @Test
    void shouldHandleRandomUrl() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(RANDOM_URL);
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertNull(urlParser.getPart(UrlPart.VERSIONS));
        assertNull(urlParser.getPart(UrlPart.REPRESENTATIONS));
        assertNull(urlParser.getPart(UrlPart.RECORDS));
        assertNull(urlParser.getPart(UrlPart.DATA_PROVIDERS));
        assertNull(urlParser.getPart(UrlPart.DATA_SETS));
        assertNull(urlParser.getPart(UrlPart.FILES));
    }

    @Test
    void shouldMarkGivenUrlAsWrongUrl() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_WITH_MISPLACED_PARTS);

        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        assertFalse(urlParser.isUrlToRepresentationVersions());

        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentationVersion());
    }

    @Test
    void shouldMarkGivenUrlAsUrlToFile() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_FILE);
        assertTrue(urlParser.isUrlToRepresentationVersionFile());

        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        //        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        assertFalse(urlParser.isUrlToRepresentationVersions());

    }

    @Test
    void shouldMarkGivenUrlAsUrlToFilesList() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_FILES_LIST);
        assertTrue(urlParser.isUrlToRepresentationVersionFiles());

        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        //        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        assertFalse(urlParser.isUrlToRepresentationVersions());
    }

    @Test
    void shouldMarkGivenUrlAsUrlToVersionsList() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_VERSIONS_LIST);
        assertTrue(urlParser.isUrlToRepresentationVersions());

        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        //        assertFalse(urlParser.isUrlToRepresentationVersions());
    }

    @Test
    void shouldMarkGivenUrlAsUrlToVersion() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_VERSION);
        assertTrue(urlParser.isUrlToRepresentationVersion());

        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentations());
        //        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        assertFalse(urlParser.isUrlToRepresentationVersions());
    }

    @Test
    void shouldMarkGivenUrlAsUrlToRepresentation() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_REPRESENTATION);
        assertTrue(urlParser.isUrlToRepresentation());

        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToDatasetsList());
        //        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        assertFalse(urlParser.isUrlToRepresentationVersions());
    }

    @Test
    void shouldMarkGivenUrlAsUrlToRepresentationsList() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_REPRESENTATIONS_LIST);
        assertTrue(urlParser.isUrlToRepresentations());

        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToRepresentation());
        //        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        assertFalse(urlParser.isUrlToRepresentationVersions());
    }

    @Test
    void shouldMarkGivenUrlAsUrlToCloudId() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_CLOUDID);
        assertTrue(urlParser.isUrlToCloudId());

        //        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        assertFalse(urlParser.isUrlToRepresentationVersions());
    }

    @Test
    void shouldMarkGivenUrlAsUrlToDataSetsList() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_DATASETS_LIST);
        assertTrue(urlParser.isUrlToDatasetsList());

        assertFalse(urlParser.isUrlToCloudId());
        assertFalse(urlParser.isUrlToDataset());
        //        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        assertFalse(urlParser.isUrlToRepresentationVersions());
    }

    @Test
    void shouldMarkGivenUrlAsUrlToDataSet() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_DATASET);
        assertTrue(urlParser.isUrlToDataset());

        assertFalse(urlParser.isUrlToCloudId());
        //        assertFalse(urlParser.isUrlToDataset());
        assertFalse(urlParser.isUrlToDatasetsList());
        assertFalse(urlParser.isUrlToRepresentation());
        assertFalse(urlParser.isUrlToRepresentations());
        assertFalse(urlParser.isUrlToRepresentationVersion());
        assertFalse(urlParser.isUrlToRepresentationVersionFile());
        assertFalse(urlParser.isUrlToRepresentationVersionFiles());
        assertFalse(urlParser.isUrlToRepresentationVersions());
    }

    @Test
    void shouldReturnProperUrlPasts() throws MalformedURLException {
        UrlParser urlParser = new UrlParser(URL_TO_DATASET);
        assertEquals(urlParser.getPart(UrlPart.DATA_SETS), "sampleDataSet");
        assertEquals(urlParser.getPart(UrlPart.DATA_PROVIDERS), "sampleDP");
        assertNull(urlParser.getPart(UrlPart.VERSIONS));
        assertNull(urlParser.getPart(UrlPart.RECORDS));
        assertNull(urlParser.getPart(UrlPart.REPRESENTATIONS));

        urlParser = new UrlParser(URL_TO_FILE);
        assertNull(urlParser.getPart(UrlPart.DATA_SETS));
        assertNull(urlParser.getPart(UrlPart.DATA_PROVIDERS));

        assertEquals(urlParser.getPart(UrlPart.VERSIONS), "86318b00-6377-11e5-a1c6-90e6ba2d09ef");
        assertEquals(urlParser.getPart(UrlPart.RECORDS), "FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ");
        assertEquals(urlParser.getPart(UrlPart.REPRESENTATIONS), "TIFF");
    }

    @Test
    void shouldReturnProperVersionUrlFromFileUrl() throws MalformedURLException, UrlBuilderException {
        UrlParser urlParser = new UrlParser(URL_TO_FILE);
        assertEquals(urlParser.getVersionUrl(), URL_TO_VERSION);
    }

    @Test
    void shouldThrowExceptionWhileCreatingUrlToVersionFromWrongUrl() {
        assertThrows(UrlBuilderException.class, () -> {
            UrlParser urlParser = new UrlParser(URL_TO_CLOUDID);
            urlParser.getVersionUrl();
        });
    }

    @Test
    void shouldReturnProperVersionsUrlFromFileUrl() throws MalformedURLException, UrlBuilderException {
        UrlParser urlParser = new UrlParser(URL_TO_FILE);
        assertEquals(urlParser.getVersionsUrl(), URL_TO_VERSIONS_LIST);
    }

    @Test
    void shouldThrowExceptionWhileTryingToGetUrlToVersionFromDatasetUrl() {
        assertThrows(UrlBuilderException.class, () -> {
            UrlParser urlParser = new UrlParser(URL_TO_DATASET);
            urlParser.getVersionsUrl();
        });
    }

    @Test
    void shouldReturnProperDatasetsUrlFromDataSetUrl() throws MalformedURLException, UrlBuilderException {
        UrlParser urlParser = new UrlParser(URL_TO_DATASET);
        assertEquals(urlParser.getDataSetsUrl(), URL_TO_DATASETS_LIST);

    }
}