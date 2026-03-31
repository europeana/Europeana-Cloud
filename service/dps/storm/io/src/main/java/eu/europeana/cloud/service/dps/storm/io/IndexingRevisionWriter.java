package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.notification.NotificationTuple;
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
  protected void addRevisionAndEmit(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    LOGGER.info("{} executed", getClass().getSimpleName());
    try {
      addRevisionToSpecificResource(commonTaskTuple, commonTaskTuple.getRecordUri());
      emitSuccessNotificationForIndexing(anchorTuple, commonTaskTuple);
    } catch (MalformedURLException e) {
      LOGGER.error("URL is malformed: {}", commonTaskTuple.getRecordUri());
      emitErrorNotification(anchorTuple, commonTaskTuple, e.getMessage(), "The cause of the error is:" + e.getCause());
    } catch (MCSException | DriverException e) {
        LOGGER.warn("Error while communicating with MCS {}", e.getMessage());
        emitErrorNotification(anchorTuple, commonTaskTuple, e.getMessage(), "The cause of the error is:" + e.getCause());
    }
  }

  protected void emitSuccessNotificationForIndexing(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
      NotificationTuple nt = NotificationTuple.prepareIndexingNotification(
              commonTaskTuple,
              RecordState.SUCCESS,
              successNotificationMessage,
              "");
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
  }


}

