package eu.europeana.cloud.service.dps.http;

import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Creates URL for file that was prepared for the HTTP topology. It will be used by the topology to download the file
 * and import it to the eCloud.
 *
 */
public class FileURLCreator {

    private static final String DIRECTORY = "/http_harvest/";
    private final String machineLocation;

    public FileURLCreator(String machineLocation) {
        this.machineLocation = machineLocation;
    }

    public String generateUrlFor(DpsTask dpsTask, String fileName) {
        return machineLocation + DIRECTORY + "task_" + dpsTask.getTaskId() + "/" + fileName;
    }
}
