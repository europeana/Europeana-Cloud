package eu.europeana.cloud.service.dps.http;

import eu.europeana.cloud.common.properties.FileServerProperties;
import org.jspecify.annotations.NonNull;
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
  private final FileServerProperties fileServerProperties;

  @Value("${harvestingTasksDir}")
  private String harvestingTasksDir;

  public FileURLCreator(String fileServerLocationExternal, String fileServerLocationInternal) {
    if (fileServerLocationExternal == null) {
      throw new NullPointerException("apacheLocationExternal parameter can not be null");
    }
    if (fileServerLocationInternal == null) {
      throw new NullPointerException("apacheLocationInternal parameter can not be null");
    }
    this.fileServerProperties = new FileServerProperties(fileServerLocationExternal, fileServerLocationInternal);
  }

  /**
   * Generates external URL for the file located in the given location
   *
   * @param file location of the file for which URL will be generated
   * @return URL to the file
   * @throws UnsupportedEncodingException when the encoding process will fail
   */
  public String generateExternalUrlFor(Path file) throws UnsupportedEncodingException {
    StringBuilder resultURL = new StringBuilder(fileServerProperties.getLocationExternal() + DIRECTORY);
    return generateURL(file, resultURL);
  }

  /**
   * Generates internal URL for the file located in the given location
   *
   * @param file location of the file for which URL will be generated
   * @return URL to the file
   * @throws UnsupportedEncodingException when the encoding process will fail
   */
  public String generateInternalUrlFor(Path file) throws UnsupportedEncodingException {
    StringBuilder resultURL = new StringBuilder(fileServerProperties.getLocationInternal() + DIRECTORY);
    return generateURL(file, resultURL);
  }

  private @NonNull String generateURL(Path file, StringBuilder resultURL) throws UnsupportedEncodingException {
    Path relative = Paths.get(harvestingTasksDir).relativize(file);
    resultURL.append(encodedUrlParts(relative));
    return resultURL.toString();
  }

  public String convertToExternalUrl(String url) {
    return fileServerProperties.convertToExternalUrl(url);
  }

  public String convertToInternalUrl(String url) {
    return fileServerProperties.convertToInternalUrl(url);
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
