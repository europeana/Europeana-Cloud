package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.FileDataChecker;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Instant;


/**
 * Read file/files from MCS and every file emits as separate {@link StormTaskTuple}.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ReadFileBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileBolt.class);

  /**
   * Properties to connect to eCloud
   */
  private final String ecloudMcsAddress;
  private final String ecloudMcsUser;
  private final String ecloudMcsUserPassword;
  protected transient FileServiceClient fileClient;

  public ReadFileBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress,
      String ecloudMcsUser, String ecloudMcsUserPassword) {
    super(cassandraProperties);
    this.ecloudMcsAddress = ecloudMcsAddress;
    this.ecloudMcsUser = ecloudMcsUser;
    this.ecloudMcsUserPassword = ecloudMcsUserPassword;
  }

  @Override
  public void prepare() {
    fileClient = new FileServiceClient(ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
  }

  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple t) {
    final String file = t.getParameters().get(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
    t.setFileUrl(file);
    try (InputStream is = getFileStreamByStormTuple(t)) {
      t.setFileData(is);
      if (FileDataChecker.isFileDataNullOrBlank(t.getFileData())) {
        LOGGER.warn("Read file data is null or blank!");
      }
      outputCollector.emit(anchorTuple, t.toStormTuple());
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (RepresentationNotExistsException | FileNotExistsException |
             WrongContentRangeException ex) {
        LOGGER.warn("Can not retrieve file at {}", file);
        emitErrorNotification(anchorTuple, t, "Can not retrieve file",
                "The cause of the error is:" + ex.getCause());
        outputCollector.ack(anchorTuple);
    } catch (Exception ex) {
        LOGGER.error("ReadFileBolt error: {}", ex.getMessage());
        emitErrorNotification(anchorTuple, t, ex.getMessage(),
                "The cause of the error is:" + ex.getCause());
        outputCollector.ack(anchorTuple);
    }
  }

  private InputStream getFile(FileServiceClient fileClient, String file) throws Exception {
    return RetryableMethodExecutor.executeOnRest("Error while getting a file", () ->
        fileClient.getFile(file));
  }

  protected InputStream getFileStreamByStormTuple(StormTaskTuple stormTaskTuple) throws Exception {
    Instant processingStartTime = Instant.now();
    final String file = stormTaskTuple.getParameters().get(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
    LOGGER.info("Downloading the following file: {}", file);
    stormTaskTuple.setFileUrl(file);
    InputStream downloadedFile = getFile(fileClient, file);
    LOGGER.info("File downloaded in {}ms", Clock.millisecondsSince(processingStartTime));
    return downloadedFile;
  }

}
