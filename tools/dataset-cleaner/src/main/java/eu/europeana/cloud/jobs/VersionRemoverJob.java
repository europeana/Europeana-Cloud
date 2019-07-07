package eu.europeana.cloud.jobs;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * Created by Tarek on 8/3/2018.
 */
public class VersionRemoverJob implements Callable<String> {
    private static final int DEFAULT_RETRIES = 3;
    public static final int SLEEP_TIME = 5000;

    private Representation representation;
    private RecordServiceClient recordServiceClient;

    private static final Logger LOGGER = Logger.getLogger(VersionRemoverJob.class);


    public VersionRemoverJob(RecordServiceClient recordServiceClient,
                             Representation representation) {

        this.recordServiceClient = recordServiceClient;
        this.representation = representation;
    }

    @Override
    public String call() throws Exception {
        removeVersion();
        return "The version " + representation.getUri() + " was cleaned successfully";
    }

    /*
    This method should remove all the version files, revisions and unassign it from all the dataSets and Finally Remove the representation version entirely
     */
    private void removeVersion() throws Exception {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                recordServiceClient.deleteRepresentation(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());
                break;
            } catch (Exception e) {
                if (--retries > 0) {
                    waitForSpecificTime();
                } else {
                    throw new Exception("Error while removing representation version " + representation.getUri() + " from dataSet because of " + e.getMessage());
                }
            }
        }
    }

    private void waitForSpecificTime() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }
}
