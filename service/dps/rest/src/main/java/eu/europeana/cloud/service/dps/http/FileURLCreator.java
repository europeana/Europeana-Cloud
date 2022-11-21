package eu.europeana.cloud.service.dps.http;

import org.springframework.beans.factory.annotation.Value;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Creates URL for file that was prepared for the HTTP topology. It will be used by the topology to download the file and import
 * it to the eCloud.
 */
public class FileURLCreator {

  private static final String DIRECTORY = "/http_harvest";
  private final String machineLocation;

  @Value("${harvestingTasksDir}")
  private String harvestingTasksDir;

  public FileURLCreator(String machineLocation) {
    if (machineLocation == null) {
      throw new NullPointerException("machineLocation parameter can not be null");
    }
    this.machineLocation = machineLocation;
  }

  /**
   * Generates URL for the file located in the given location
   *
   * @param file location of the file for which URL will be generated
   * @return URL to the file
   * @throws UnsupportedEncodingException when the encoding process will fail
   */
  public String generateUrlFor(Path file) throws UnsupportedEncodingException {
    StringBuilder resultURL = new StringBuilder(machineLocation + DIRECTORY);
    Path relative = Paths.get(harvestingTasksDir).relativize(file);
    resultURL.append(encodedUrlParts(relative));
    return resultURL.toString();
  }

  private String encodedUrlParts(Path path) throws UnsupportedEncodingException {
    StringBuilder resultURL = new StringBuilder();
    String[] parts = path.toString().split("/");
    for (String part : parts) {
      resultURL.append("/").append(URLEncoder.encode(part, StandardCharsets.UTF_8.toString()));
    }
    return resultURL.toString();
  }
}
