package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.tuple.Tuple;

import java.net.MalformedURLException;

/**
 * Created by Tarek on 9/24/2019.
 */
public class IndexingRevisionWriter extends RevisionWriterBolt {

  private static final long serialVersionUID = 1L;

  private final String successNotificationMessage;

  public IndexingRevisionWriter(CassandraProperties cassandraProperties,
      String ecloudMcsAddress,
      String ecloudMcsUser,
      String ecloudMcsUserPassword,
      String successNotificationMessage) {
    super(cassandraProperties, ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
    this.successNotificationMessage = successNotificationMessage;
  }

  @Override
  protected void addRevisionAndEmit(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    LOGGER.info("{} executed", getClass().getSimpleName());
    try {
      addRevisionToSpecificResource(stormTaskTuple, stormTaskTuple.getFileUrl());
      emitSuccessNotificationForIndexing(anchorTuple, stormTaskTuple);
    } catch (MalformedURLException e) {
      LOGGER.error("URL is malformed: {}", stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
        emitErrorNotification(anchorTuple, stormTaskTuple, e.getMessage(), "The cause of the error is:" + e.getCause());
    } catch (MCSException | DriverException e) {
        LOGGER.warn("Error while communicating with MCS {}", e.getMessage());
        emitErrorNotification(anchorTuple, stormTaskTuple, e.getMessage(), "The cause of the error is:" + e.getCause());
    }
  }

  protected void emitSuccessNotificationForIndexing(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
      NotificationTuple nt = NotificationTuple.prepareIndexingNotification(
              stormTaskTuple,
              RecordState.SUCCESS,
              successNotificationMessage,
              "");
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
  }


}

