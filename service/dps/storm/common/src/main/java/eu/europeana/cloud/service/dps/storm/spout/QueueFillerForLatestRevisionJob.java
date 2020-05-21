package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.spout.SpoutOutputCollector;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by Tarek on 9/12/2018.
 */
public class QueueFillerForLatestRevisionJob extends QueueFillerJobForRevision {

    private List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList;

    public QueueFillerForLatestRevisionJob(FileServiceClient fileServiceClient, RecordServiceClient recordServiceClient, SpoutOutputCollector collector, TaskStatusChecker taskStatusChecker, ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls, StormTaskTuple stormTaskTuple, String representationName, String revisionName, String revisionProvider, List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList) {
        super(recordServiceClient, fileServiceClient, representationName, stormTaskTuple, revisionName, revisionProvider, taskStatusChecker, collector, tuplesWithFileUrls);
        this.cloudIdAndTimestampResponseList = cloudIdAndTimestampResponseList;
    }

    public int fillTheQueue() throws MCSException {
        int count = 0;
        for (CloudIdAndTimestampResponse cloudIdAndTimestampResponse : cloudIdAndTimestampResponseList) {
            List<Representation> representations = getRepresentationByRevision(recordServiceClient, representationName, revisionName, revisionProvider, DateHelper.getUTCDateString(cloudIdAndTimestampResponse.getRevisionTimestamp()), cloudIdAndTimestampResponse.getCloudId());
            for (Representation representation: representations){
                count += addTupleToQueue(stormTaskTuple, fileServiceClient, representation);
            }
        }
        return count;
    }
}
