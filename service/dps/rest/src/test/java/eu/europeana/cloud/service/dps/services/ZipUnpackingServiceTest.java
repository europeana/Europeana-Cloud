package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.service.dps.http.CompressionExtensionNotRecognizedException;
import eu.europeana.cloud.service.dps.http.FileUnpackingService;
import eu.europeana.cloud.service.dps.http.ZipUnpackingService;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ZipUnpackingServiceTest {

    private FileUnpackingService zipUnpackingService;
    private final static String DESTINATION_DIR = "src/test/resources/__files/";
    private final static int XML_FILES_COUNT = 13;
    private final static String FILE_NAME = "zipFileWithNestedZipFiles";
    private final static String FILE_NAME2 = "zipFileWithNestedFolders";
    private final static String FILE_NAME3 = "ZipFilesWithMixedCompressedFiles";
    private final static String DEFAULT_DESTINATION_NAME = "zipFile";
    private final static String XML_TYPE = "xml";
    public static final String ZIP_EXTENSION = ".zip";

    @Before
    public void init() {
        zipUnpackingService = new ZipUnpackingService();
    }

    @Test
    public void shouldUnpackTheZipFilesRecursively() throws CompressionExtensionNotRecognizedException, IOException {
        zipUnpackingService.unpackFile(DESTINATION_DIR + FILE_NAME + ZIP_EXTENSION, DESTINATION_DIR);
        Collection files = getXMLFiles(DESTINATION_DIR + DEFAULT_DESTINATION_NAME);
        assertNotNull(files);
        assertEquals(XML_FILES_COUNT,files.size());
    }

    @Test
    public void shouldUnpackTheZipFilesWithNestedFoldersRecursively() throws CompressionExtensionNotRecognizedException, IOException {
        zipUnpackingService.unpackFile(DESTINATION_DIR + FILE_NAME2 + ZIP_EXTENSION, DESTINATION_DIR);
        Collection files = getXMLFiles(DESTINATION_DIR + DEFAULT_DESTINATION_NAME);
        assertNotNull(files);
        assertEquals(XML_FILES_COUNT,files.size());
    }

    @Test
    public void shouldUnpackTheZipFilesWithNestedMixedCompressedFiles() throws CompressionExtensionNotRecognizedException, IOException {
        zipUnpackingService.unpackFile(DESTINATION_DIR + FILE_NAME3 + ZIP_EXTENSION, DESTINATION_DIR);
        Collection files = getXMLFiles(DESTINATION_DIR + DEFAULT_DESTINATION_NAME);
        assertNotNull(files);
        assertEquals(XML_FILES_COUNT,files.size());
    }


    private Collection getXMLFiles(String folderLocation) {
        String[] types = {XML_TYPE};
        return (Collection) FileUtils.listFiles(
                new File(folderLocation),
                types,
                true
        );
    }

    @After
    public void cleanUp() throws IOException {
        FileUtils.forceDelete(new File(DESTINATION_DIR + DEFAULT_DESTINATION_NAME));
    }


}