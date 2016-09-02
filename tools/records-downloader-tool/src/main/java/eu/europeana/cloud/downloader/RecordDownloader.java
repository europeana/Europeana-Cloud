package eu.europeana.cloud.downloader;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.exception.RepresentationNotFoundException;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.util.FileUtil;
import eu.europeana.cloud.util.MimeTypeHelper;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypeException;

import java.io.*;


/**
 * Created by Tarek on 9/1/2016.
 */
public class RecordDownloader {
    DataSetServiceClient dataSetServiceClient;
    FileServiceClient fileServiceClient;


    public RecordDownloader(DataSetServiceClient dataSetServiceClient, FileServiceClient fileServiceClient) {
        this.dataSetServiceClient = dataSetServiceClient;
        this.fileServiceClient = fileServiceClient;
    }

    /**
     * download files inside a dataSet which have specific representation and store them inside a newly created folder
     *
     * @param providerId         provider id
     * @param datasetName        The name of the dataSet
     * @param representationName representation name
     */
    public String downloadFilesFromDataSet(String providerId, String datasetName, String representationName) throws MimeTypeException, IOException, DriverException, RepresentationNotFoundException, MCSException {
        String folderPath = FileUtil.createFolder();
        RepresentationIterator iterator = dataSetServiceClient.getRepresentationIterator(providerId, datasetName);
        boolean representationIsFound = false;
        while (iterator.hasNext()) {
            Representation representation = iterator.next();
            if (representation.getRepresentationName().equals(representationName)) {
                representationIsFound = true;
                for (File file : representation.getFiles()) {
                    final String fileUrl = fileServiceClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName()).toString();
                    InputStream inputStream = fileServiceClient.getFile(fileUrl);
                    String extension = getExtension(inputStream);
                    FileUtil.persistStreamToFile(inputStream, folderPath, file.getFileName(), extension);
                }
            }
        }
        if (!representationIsFound)
            throw new RepresentationNotFoundException("The representation " + representationName + " was not found inside the dataset");
        return folderPath;
    }

    private String getExtension(InputStream inputStream) throws MimeTypeException, IOException {
        MediaType mimeType = MimeTypeHelper.getMediaTypeFromStream(inputStream);
        String extension = MimeTypeHelper.getExtension(mimeType);
        return extension;
    }


}
