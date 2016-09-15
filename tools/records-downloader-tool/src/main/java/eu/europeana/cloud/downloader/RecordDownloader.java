package eu.europeana.cloud.downloader;

import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.exception.RepresentationNotFoundException;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.util.FileUtil;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;


/**
 * Created by Tarek on 9/1/2016.
 */
public class RecordDownloader {
    private DataSetServiceClient dataSetServiceClient;
    private FileServiceClient fileServiceClient;


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
    public final String downloadFilesFromDataSet(String providerId, String datasetName, String representationName, int threadsCount) throws InterruptedException, ExecutionException, IOException, DriverException, RepresentationNotFoundException {
        ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);
        final String folderPath = FileUtil.createFolder();
        boolean isSuccess = false;
        try {
            RepresentationIterator iterator = dataSetServiceClient.getRepresentationIterator(providerId, datasetName);
            boolean representationIsFound = false;
            while (iterator.hasNext()) {
                final Representation representation = iterator.next();
                if (representation.getRepresentationName().equals(representationName)) {
                    representationIsFound = true;
                    downloadFilesInsideRepresentation(executorService, representation, folderPath);
                }
            }
            if (!representationIsFound)
                throw new RepresentationNotFoundException("The representation " + representationName + " was not found inside the dataset: " + datasetName);
            isSuccess = true;
            return folderPath;
        } finally {
            executorService.shutdown();
            if (!isSuccess)
                FileUtils.deleteDirectory(new java.io.File(folderPath));


        }
    }

    private void downloadFilesInsideRepresentation(ExecutorService executorService, Representation representation, String folderPath) throws InterruptedException, ExecutionException, DriverException {
        final Set<Callable<Void>> fileDownloaderJobs = new HashSet<>();
        for (final File file : representation.getFiles()) {
            fileDownloaderJobs.add(new FileDownloaderJob(fileServiceClient, file.getFileName(), representation, folderPath));
        }
        List<Future<Void>> results = executorService.invokeAll(fileDownloaderJobs);
        for (Future<Void> future : results) {
            future.get();
        }
    }


}
