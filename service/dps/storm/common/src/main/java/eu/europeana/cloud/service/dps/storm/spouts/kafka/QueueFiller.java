package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

/**
 * Created by Tarek on 9/14/2018.
 */
public class QueueFiller {
    private TaskStatusChecker taskStatusChecker;
    private SpoutOutputCollector collector;
    ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls;
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueFiller.class);

     QueueFiller(TaskStatusChecker taskStatusChecker, SpoutOutputCollector collector, ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls) {
        this.taskStatusChecker = taskStatusChecker;
        this.collector = collector;
        this.tuplesWithFileUrls = tuplesWithFileUrls;
    }

     int addTupleToQueue(StormTaskTuple stormTaskTuple, FileServiceClient fileServiceClient, Representation representation) {
        int count = 0;
        final long taskId = stormTaskTuple.getTaskId();
        if (representation != null) {
            for (eu.europeana.cloud.common.model.File file : representation.getFiles()) {
                String fileUrl = "";
                if (!taskStatusChecker.hasKillFlag(taskId)) {
                    try {
                        fileUrl = fileServiceClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName()).toString();
                        final StormTaskTuple fileTuple = buildNextStormTuple(stormTaskTuple, fileUrl);
                        tuplesWithFileUrls.put(fileTuple);
                        count++;
                    } catch (Exception e) {
                        LOGGER.warn("Error while getting File URI from MCS {}", e.getMessage());
                        count++;
                        emitErrorNotification(taskId, fileUrl, "Error while getting File URI from MCS " + e.getMessage(), "");
                    }
                } else
                    break;
            }
        } else {
            LOGGER.warn("Problem while reading representation");
        }
        return count;
    }

     private void emitErrorNotification(long taskId, String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.ERROR, message, additionalInformations);
        collector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

     private StormTaskTuple buildNextStormTuple(StormTaskTuple stormTaskTuple, String fileUrl) {
        StormTaskTuple fileTuple = new Cloner().deepClone(stormTaskTuple);
        fileTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, fileUrl);
        return fileTuple;
    }

}
