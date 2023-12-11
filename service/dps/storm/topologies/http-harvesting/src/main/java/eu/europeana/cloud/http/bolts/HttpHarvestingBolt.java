package eu.europeana.cloud.http.bolts;

import static eu.europeana.metis.utils.TempFileUtils.createSecureTempFile;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.harvesting.commons.IdentifierSupplier;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.commons.io.FilenameUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

public class HttpHarvestingBolt extends AbstractDpsBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpHarvestingBolt.class);
  private static final int SLEEP_TIME_BETWEEN_RETRIES_MS = 30_000; //this constant is not final for test purpose
  private transient IdentifierSupplier identifierSupplier;
  private transient HttpClient httpClient;

  public HttpHarvestingBolt(CassandraProperties cassandraProperties) {
    super(cassandraProperties);
  }

  @Override
  public void prepare() {
    identifierSupplier = new IdentifierSupplier();
    httpClient = HttpClient.newBuilder().build();
  }


  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple tuple) {
    try {
      LOGGER.info("Starting http harvesting for url: {}", tuple.getFileUrl());
      harvestRecord(tuple);

      outputCollector.emit(anchorTuple, tuple.toStormTuple());
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (InterruptedException e) {
      LOGGER.error("Bolt thread is being interrupted!", e);
      Thread.currentThread().interrupt();
      outputCollector.fail(anchorTuple);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      emitErrorNotification(anchorTuple, tuple, "Error while reading a file",
              "Can't read file: " + tuple.getFileUrl() + " because of " + e.getMessage());
      outputCollector.ack(anchorTuple);
    }
  }

  private void harvestRecord(StormTaskTuple tuple) throws Exception {
    HttpResponse<byte[]> response = tryLoadHttpFileCoupleOfTimes(tuple);
    byte[] fileContent = response.body();
    tuple.setFileData(fileContent);
    tuple.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, probeMimeType(tuple.getFileUrl(), fileContent));
    identifierSupplier.prepareIdentifiers(tuple);
  }

  private String probeMimeType(String fileUrl, byte[] fileContent) throws IOException {
    String extension = FilenameUtils.getExtension(fileUrl);
    Path tempFile = createSecureTempFile("http_harvest", "." + extension);
    Files.write(tempFile, fileContent);
    String mimeType = Files.probeContentType(tempFile);
    Files.delete(tempFile);
    return mimeType;
  }

  private HttpResponse<byte[]> tryLoadHttpFileCoupleOfTimes(StormTaskTuple tuple) throws Exception {
    //Because data are always loaded from the same given application server, relatively big retry count,
    //and time is used to assure some resistance for server, inaccessibility.
    return RetryableMethodExecutor.<HttpResponse<byte[]>, Exception>
                                      execute("Loading file by http failed!", 6, SLEEP_TIME_BETWEEN_RETRIES_MS,
        () -> loadHttpFile(tuple));
  }

  private HttpResponse<byte[]> loadHttpFile(StormTaskTuple tuple) throws IOException, InterruptedException {
    HttpRequest request = HttpRequest.newBuilder(URI.create(tuple.getFileUrl())).GET().build();
    HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    if (response.statusCode() != 200) {
      throw new IOException("Bad return status code: " + response.statusCode());
    }
    return response;
  }

}
