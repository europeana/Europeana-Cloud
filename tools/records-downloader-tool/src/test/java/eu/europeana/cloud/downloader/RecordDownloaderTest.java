package eu.europeana.cloud.downloader;

import eu.europeana.cloud.TestConstantsHelper;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.exception.RepresentationNotFoundException;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class RecordDownloaderTest implements TestConstantsHelper {

    RecordDownloader recordDownloader;
    FileServiceClient fileServiceClient;
    DataSetServiceClient dataSetServiceClient;
    RepresentationIterator representationIterator;
    InputStream inputStream;
    InputStream inputStream2;

    @Before
    public void init() throws Exception {
        fileServiceClient = mock(FileServiceClient.class);
        dataSetServiceClient = mock(DataSetServiceClient.class);
        representationIterator = mock(RepresentationIterator.class);
        recordDownloader = new RecordDownloader(dataSetServiceClient, fileServiceClient);

    }


    @Test
    public void shouldSuccessfullyDownloadTwoRecords() throws Exception {
        Representation representation = prepareRepresentation();
        inputStream = IOUtils.toInputStream("some test data for my input stream");
        inputStream2 = IOUtils.toInputStream("some test data for my input stream");
        when(dataSetServiceClient.getRepresentationIterator(anyString(), anyString())).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(CLOUD_ID, REPRESENTATION_NAME, VERSION, FILE)).thenReturn(new URI(FILE_URL));
        when(fileServiceClient.getFileUri(CLOUD_ID, REPRESENTATION_NAME, VERSION, FILE + "2")).thenReturn(new URI(FILE_URL2));
        when(fileServiceClient.getFile(FILE_URL)).thenReturn(inputStream);
        when(fileServiceClient.getFile(FILE_URL2)).thenReturn(inputStream2);
        String folderPtah = recordDownloader.downloadFilesFromDataSet(DATA_PROVIDER, DATASET_NAME, REPRESENTATION_NAME,1);
        assertNotNull(folderPtah);
        java.io.File folder = new java.io.File(folderPtah);
        assert (folder.isDirectory());
        assertEquals(folder.list().length, 2);
        FileUtils.forceDelete(new java.io.File(folderPtah));

    }

    @Test(expected = RepresentationNotFoundException.class)
    public void shouldThrowRepresentationNotFoundException() throws Exception {
        when(dataSetServiceClient.getRepresentationIterator(anyString(), anyString())).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(false, false);
        recordDownloader.downloadFilesFromDataSet(DATA_PROVIDER, DATASET_NAME, EMPTY_REPRESENTATION,1);

    }

    private Representation prepareRepresentation() throws URISyntaxException

    {
        List<File> files = new ArrayList<>();
        List<Revision> revisions = new ArrayList<>();
        files.add(new File("fileName", "text/plain", "md5", "1", 5, new URI(FILE_URL)));
        files.add(new File("fileName2", "text/plain", "md5", "1", 5, new URI(FILE_URL2)));
        Representation representation = new Representation(CLOUD_ID, REPRESENTATION_NAME, VERSION, new URI(FILE_URL), new URI(FILE_URL), DATA_PROVIDER, files, revisions, false, new Date());
        return representation;
    }


}
