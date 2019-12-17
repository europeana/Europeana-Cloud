package eu.europeana.cloud.jobs;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.data.RevisionInformation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Date;
import java.util.List;

/**
 * Created by Tarek on 7/15/2019.
 */
public class RevisionRemoverJob implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(RevisionRemoverJob.class);

    private DataSetServiceClient dataSetServiceClient;
    private RecordServiceClient recordServiceClient;
    private RevisionServiceClient revisionServiceClient;
    private RevisionInformation revisionInformation;
    private final int threadCount;
    private final AtomicInteger completed = new AtomicInteger(0);
    private final AtomicInteger total = new AtomicInteger(-1);

    private static final int SLEEP_TIME = 600000;

    public RevisionRemoverJob(DataSetServiceClient dataSetServiceClient,
            RecordServiceClient recordServiceClient, RevisionInformation revisionInformation,
            RevisionServiceClient revisionServiceClient, int threadCount) {
        this.dataSetServiceClient = dataSetServiceClient;
        this.recordServiceClient = recordServiceClient;
        this.revisionServiceClient = revisionServiceClient;
        this.revisionInformation = revisionInformation;
        this.threadCount = threadCount;
    }

    @Override
    public void run() {

        // Set up an executor service with the correct thread count.
        ExecutorService executorService = Executors.newFixedThreadPool(this.threadCount);

        // Loop through the cloud IDs, using result slicing (pagination).
        String startFrom = null;
        int totalCounter = 0;
        do {

            // Get the next result slice. Check that there is content.
            ResultSlice<CloudTagsResponse> resultSlice = null;
            try {
                resultSlice = dataSetServiceClient.getDataSetRevisionsChunk(
                        revisionInformation.getProviderId(),
                        revisionInformation.getDataSet(),
                        revisionInformation.getRepresentationName(),
                        revisionInformation.getRevisionName(),
                        revisionInformation.getRevisionProvider(),
                        revisionInformation.getRevisionTimeStamp(), startFrom, null);
                if (resultSlice == null || resultSlice.getResults() == null) {
                    LOGGER.error("Getting cloud IDs and revision tags: result chunk obtained but is empty.");
                }
            } catch (MCSException | RuntimeException e) {
                logConnectionException(e);
            }

            // If we have not obtained any results, we are done.
            if (resultSlice == null || resultSlice.getResults() == null) {
                executorService.shutdownNow();
                break;
            }

            // Submit each response to the executor service. As soon as an exception occurs, we stop processing.
            try {
                for (CloudTagsResponse response : resultSlice.getResults()) {
                    if (!executorService.isShutdown()) {
                        executorService.submit(() -> removeRevision(response, executorService::shutdownNow));
                    }
                }
            } catch (RejectedExecutionException e) {
                LOGGER.info("Could not submit task: probably the thread pool was closed due to an earlier problem. ", e);
                break;
            }

            // Go to next page.
            startFrom = resultSlice.getNextSlice();
            totalCounter += resultSlice.getResults().size();
        }
        while (startFrom != null && !executorService.isShutdown());
        total.set(totalCounter);

        // Trigger an orderly shutdown and wait for all tasks to finish.
        final boolean success = !executorService.isShutdown();
        executorService.shutdown();
        try {
            executorService.awaitTermination(100, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // If all went well, we log this.
        if (success) {
            LOGGER.info("The removal of revision " + revisionInformation.getRevisionName() + "_" + revisionInformation.getRevisionProvider() + "_" + revisionInformation.getRevisionTimeStamp() +
                    " inside dataset: " + revisionInformation.getDataSet() + "_" + revisionInformation.getProviderId() + " was a success! Removed " + total.get() + " records.");
            LOGGER.info("***********************");
        }
    }

    private void logConnectionException(Exception e) {
        LOGGER.error("The removal of revision " + revisionInformation.getRevisionName() + "_" + revisionInformation.getRevisionProvider() + "_" + revisionInformation.getRevisionTimeStamp() +
                " inside dataset: " + revisionInformation.getDataSet() + "_" + revisionInformation.getProviderId() + " was a failure. Please remove it again", e);
    }

    private void removeRevision(CloudTagsResponse cloudTagsResponse, Runnable exceptionListener) {
        try {

            // Get the representations for the cloud id and revision.
            List<Representation> representations = recordServiceClient.getRepresentationsByRevision(
                    cloudTagsResponse.getCloudId(),
                    revisionInformation.getRepresentationName(),
                    revisionInformation.getRevisionName(),
                    revisionInformation.getRevisionProvider(),
                    revisionInformation.getRevisionTimeStamp());

            // Remove the revisions.
            for (Representation representation : representations) {
                if (representation.getRevisions().size() == 1) {
                    removeRepresentationWithFilesAndRevisions(cloudTagsResponse, representation);
                    break; // TODO JV Should this break statement be here? It seems like a bug.
                } else {
                    removeRevisionsOnly(representation);
                }
            }

            // Update the counter and perform logging.
            final int currentlyCompleted = completed.incrementAndGet();
            if (currentlyCompleted % 100 == 0) {
                final int currentTotal = total.get();
                final String totalString = currentTotal < 0 ? "" : (" of " + currentTotal);
                LOGGER.info("The removal of revision " + revisionInformation.getRevisionName() + "_" + revisionInformation.getRevisionProvider() + "_" + revisionInformation.getRevisionTimeStamp() +
                        " inside dataset: " + revisionInformation.getDataSet() + "_" + revisionInformation.getProviderId() + " is in progress! Completed " + currentlyCompleted + totalString + " records.");
            }
        } catch (MCSException | RuntimeException e) {

            // In case of problems: log the issue and trigger the exception listener.
            logConnectionException(e);
            exceptionListener.run();
        }
    }

    private void removeRevisionsOnly(Representation representation) throws MCSException {
        DateTime utc = new DateTime(revisionInformation.getRevisionTimeStamp(), DateTimeZone.UTC);
        Date revisionDate = utc.toDate();
        for (Revision revision : representation.getRevisions()) {
            if (revisionInformation.getRevisionName().equals(revision.getRevisionName()) && revisionInformation.getRevisionProvider().equals(revision.getRevisionProviderId()) && revisionDate.getTime() == revision.getCreationTimeStamp().getTime()) {
                removeSpecificRevision(representation);
            }
        }
    }

    private void removeSpecificRevision(Representation representation) throws MCSException {
        int retries = 2;
        while (true) {
            try {
                revisionServiceClient.deleteRevision(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), revisionInformation.getRevisionName(), revisionInformation.getRevisionProvider(), revisionInformation.getRevisionTimeStamp());
                break;
            } catch (Exception e) {
                retries--;
                LOGGER.warn("Error while removing the revision." + revisionInformation.getRevisionName() + "_" + revisionInformation.getRevisionProvider() + "_" + revisionInformation.getRevisionTimeStamp() + ". Will retry after 10 minutes. Retries left: " + retries);
                waitForTheNextCall();
                if (retries <= 0)
                    throw e;
            }
        }

    }

    private void removeRepresentationWithFilesAndRevisions(CloudTagsResponse cloudTagsResponse, Representation representation) throws MCSException {
        int retries = 2;
        while (true) {
            try {
                recordServiceClient.deleteRepresentation(cloudTagsResponse.getCloudId(), representation.getRepresentationName(), representation.getVersion());
                break;
            } catch (Exception e) {
                retries--;
                LOGGER.warn("Error while removing the presentation Version. will retry after 10 minutes. Retries left: " + retries);
                waitForTheNextCall();
                if (retries <= 0)
                    throw e;
            }
        }
    }

    void setRevisionInformation(RevisionInformation revisionInformation) {
        this.revisionInformation = revisionInformation;
    }

    private void waitForTheNextCall() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }
}
