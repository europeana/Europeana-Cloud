package eu.europeana.cloud.migrator;

import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;

import static org.junit.Assert.*;

public class EuropeanaNewspapersResourceProviderTest {

    private static final String REPRESENTATION_NAME = "presentation_images";

    private static final String MAPPING_FILE_NAME = "src/test/resources/mapping.csv";

    private static final String LOCAL_LOCATIONS = "file:///$1/test1;file:///$1/test2";

    private static final String REMOTE_LOCATIONS = "http://www.test.com/test1;http://www.test.com/test2";

    private static final String MIXED_LOCATIONS = "http://www.test.com/test1;file:///c:/temp/test2";

    private static final String LOCATION_1 = "$1/test1";

    private static final String LOCATION_2 = "$1/test2";

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

    private EuropeanaNewspapersResourceProvider provider;

    private String resDir;


    @Before
    public void setUp() throws Exception {
        resDir = Paths.get(Paths.get(".").toAbsolutePath().normalize().toString(), "src/test/resources").toAbsolutePath().normalize().toString().replace("\\", "/");
    }


    @Test
    public void testGetProviderId() throws Exception {
        provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, LOCAL_LOCATIONS.replace("$1", resDir));

        String file1 = LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_1;
        assertEquals(PROVIDER_1, provider.getProviderId(file1));
        String file2 = LOCATION_2.replace("$1", resDir) + "/" + PATH_2 + "/" + FILE_2;
        assertEquals(PROVIDER_2, provider.getProviderId(file2));
    }

    @Test
    public void testGetLocalIdentifier() throws Exception {
        provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, LOCAL_LOCATIONS.replace("$1", resDir));

        String file = LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_1;
        String localId = provider.getLocalIdentifier(LOCATION_1.replace("$1", resDir), file, false);
        assertEquals(localId, LOCAL_ID_1);
        file = LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_3;
        localId = provider.getLocalIdentifier(LOCATION_1.replace("$1", resDir), file, false);
        assertEquals(localId, LOCAL_ID_2);
        file = LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_4;
        localId = provider.getLocalIdentifier(LOCATION_1.replace("$1", resDir), file, false);
        assertEquals(localId, LOCAL_ID_2);
    }

    @Test
    public void testGetLocalIdentifierReturnsNull() throws Exception {
        provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, LOCAL_LOCATIONS.replace("$1", resDir));

        // location is different
        String file1 = LOCATION_2.replace("$1", resDir) + "/" + PATH_2 + "/" + FILE_2;
        String localId = provider.getLocalIdentifier(LOCATION_1.replace("$1", resDir), file1, false);
        assertNull(localId);

        // no association between path and local identifier
        file1 = LOCATION_1.replace("$1", resDir) + "/" + PATH_NON_EXISTING + "/" + FILE_1;
        localId = provider.getLocalIdentifier(LOCATION_1.replace("$1", resDir), file1, false);
        assertNull(localId);
    }

    @Test
    public void testScan() throws Exception {
        provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, LOCAL_LOCATIONS.replace("$1", resDir));

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
        assertEquals(FilenameUtils.normalize(LOCATION_1.replace("$1", resDir)), paths.get(PROVIDER_1).get(0).getLocation());
        assertEquals(FilenameUtils.normalize(LOCATION_2.replace("$1", resDir)), paths.get(PROVIDER_2).get(0).getLocation());
        assertEquals(PROVIDER_1, paths.get(PROVIDER_1).get(0).getDataProvider());
        assertEquals(PROVIDER_2, paths.get(PROVIDER_2).get(0).getDataProvider());
        // there should be only one path in FilePaths object
        assertEquals(3, paths.get(PROVIDER_1).get(0).getFullPaths().size());
        assertEquals(1, paths.get(PROVIDER_2).get(0).getFullPaths().size());
        // path should be equal to the real one
        String path = FilenameUtils.normalize(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_1);
        assertTrue(paths.get(PROVIDER_1).get(0).getFullPaths().contains(path));
        path = FilenameUtils.normalize(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_3);
        assertTrue(paths.get(PROVIDER_1).get(0).getFullPaths().contains(path));
        path = FilenameUtils.normalize(LOCATION_1.replace("$1", resDir) + "/" + PATH_1 + "/" + FILE_4);
        assertTrue(paths.get(PROVIDER_1).get(0).getFullPaths().contains(path));
        assertEquals(FilenameUtils.normalize(LOCATION_2.replace("$1", resDir) + "/" + PATH_2 + "/" + FILE_2), FilenameUtils.normalize(paths.get(PROVIDER_2).get(0).getFullPaths().get(0)));
    }

    @Test
    public void testDetectLocalLocations() throws Exception {
        provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, LOCAL_LOCATIONS.replace("$1", resDir));

        assertTrue(provider.isLocal());
    }


    @Test
    public void testDetectRemoteLocations() throws Exception {
        provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, REMOTE_LOCATIONS);

        assertFalse(provider.isLocal());
    }


    @Test(expected = IllegalArgumentException.class)
    public void testDetectMixedLocations() throws Exception {
        // just create a new object, locations are detected in constructor
        provider = new EuropeanaNewspapersResourceProvider(REPRESENTATION_NAME, MAPPING_FILE_NAME, MIXED_LOCATIONS);
    }
}