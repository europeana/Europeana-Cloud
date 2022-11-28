package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.MalformedURLException;
import org.apache.storm.tuple.Tuple;

/**
 * Created by Tarek on 9/24/2019.
 */
public class IndexingRevisionWriter extends RevisionWriterBolt {

  private static final long serialVersionUID = 1L;

  private final String successNotificationMessage;

  public IndexingRevisionWriter(
      String ecloudMcsAddress,
      String ecloudMcsUser,
      String ecloudMcsUserPassword,
      String successNotificationMessage) {
    super(ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
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
      emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.isMarkedAsDeleted(),
          stormTaskTuple.getFileUrl(), e.getMessage(), "The cause of the error is:" + e.getCause(),
          StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
    } catch (MCSException | DriverException e) {
      LOGGER.warn("Error while communicating with MCS {}", e.getMessage());
      emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.isMarkedAsDeleted(),
          stormTaskTuple.getFileUrl(), e.getMessage(), "The cause of the error is:" + e.getCause(),
          StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
    }
  }

  protected void emitSuccessNotificationForIndexing(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    NotificationTuple nt = NotificationTuple.prepareIndexingNotification(
        stormTaskTuple.getTaskId(),
        stormTaskTuple.isMarkedAsDeleted(),
        stormTaskTuple.getFileUrl(),
        RecordState.SUCCESS,
        successNotificationMessage,
        "",
        stormTaskTuple.getParameter(PluginParameterKeys.EUROPEANA_ID),
        "",
        StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple)
    );
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
  }


}

