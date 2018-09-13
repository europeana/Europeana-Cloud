package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

/**
 * Created by Tarek on 9/12/2018.
 */
public class QueueFillerJob implements Callable<Integer> {

    private RecordServiceClient recordServiceClient;
    private FileServiceClient fileServiceClient;
    private String representationName;
    private StormTaskTuple stormTaskTuple;
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueFillerJob.class);
    private String revisionName;
    private String revisionProvider;
    private String revisionTimestamp;
    private List<CloudTagsResponse> cloudTagsResponses;
    private TaskStatusChecker taskStatusChecker;
    private SpoutOutputCollector collector;
    private ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls;

    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;

    public QueueFillerJob(FileServiceClient fileServiceClient, RecordServiceClient recordServiceClient, SpoutOutputCollector collector, TaskStatusChecker taskStatusChecker, ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls, StormTaskTuple stormTaskTuple, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, List<CloudTagsResponse> cloudTagsResponses) {
        this.recordServiceClient = recordServiceClient;
        this.fileServiceClient = fileServiceClient;
        this.representationName = representationName;
        this.revisionName = revisionName;
        this.revisionProvider = revisionProvider;
        this.revisionTimestamp = revisionTimestamp;
        this.cloudTagsResponses = cloudTagsResponses;
        this.stormTaskTuple = stormTaskTuple;
        this.taskStatusChecker = taskStatusChecker;
        this.collector = collector;
        this.tuplesWithFileUrls = tuplesWithFileUrls;
    }




    public int fillTheQueue() throws MCSException {
        int count = 0;
        for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
            Representation representation = getRepresentationByRevision(recordServiceClient, representationName, revisionName, revisionProvider, revisionTimestamp, cloudTagsResponse.getCloudId());
            count += addTupleToQueue(stormTaskTuple, fileServiceClient, representation);
        }
        return count;
    }

    private void waitForSpecificTime(int milliSecond) {
        try {
            Thread.sleep(milliSecond);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }

    private int addTupleToQueue(StormTaskTuple stormTaskTuple, FileServiceClient fileServiceClient, Representation representation) {
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

    private Representation getRepresentationByRevision(RecordServiceClient recordServiceClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String responseCloudId) throws MCSException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return recordServiceClient.getRepresentationByRevision(responseCloudId, representationName, revisionName, revisionProvider, revisionTimestamp);
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting representation revision. Retries Left{} ", retries);
                    waitForSpecificTime(SLEEP_TIME);
                } else {
                    LOGGER.error("Error while getting representation revision.");
                    throw e;
                }
            }
        }
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

    @Override
    public Integer call() throws Exception {
        return fillTheQueue();
    }
}
