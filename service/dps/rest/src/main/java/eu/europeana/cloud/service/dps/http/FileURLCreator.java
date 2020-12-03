package eu.europeana.cloud.service.dps.http;

import org.springframework.beans.factory.annotation.Value;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Creates URL for file that was prepared for the HTTP topology. It will be used by the topology to download the file
 * and import it to the eCloud.
 *
 */
public class FileURLCreator {

    private static final String DIRECTORY = "/http_harvest/";
    private final String machineLocation;

    @Value("${harvestingTasksDir}")
    private String harvestingTasksDir;

    public FileURLCreator(String machineLocation) {
        this.machineLocation = machineLocation;
    }

    public String generateUrlFor(Path file) {
        Path relative = Paths.get(harvestingTasksDir).relativize(file);
        return machineLocation + DIRECTORY + relative;
    }
}
