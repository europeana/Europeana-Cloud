package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.spout.SpoutOutputCollector;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by Tarek on 9/12/2018.
 */
public class QueueFillerForSpecificRevisionJob extends QueueFillerJobForRevision {
    private String revisionTimestamp;
    private List<CloudTagsResponse> cloudTagsResponses;

    public QueueFillerForSpecificRevisionJob(RecordServiceClient recordServiceClient, SpoutOutputCollector collector, TaskStatusChecker taskStatusChecker, ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls, StormTaskTuple stormTaskTuple, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, List<CloudTagsResponse> cloudTagsResponses) {
        super(recordServiceClient, representationName, stormTaskTuple, revisionName, revisionProvider, taskStatusChecker, collector, tuplesWithFileUrls);
        this.cloudTagsResponses = cloudTagsResponses;
        this.revisionTimestamp = revisionTimestamp;
    }

    public int fillTheQueue() throws MCSException {
        int count = 0;
        for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
            List<Representation> representations = getRepresentationByRevision(recordServiceClient, representationName, revisionName, revisionProvider, revisionTimestamp, cloudTagsResponse.getCloudId());
            for (Representation representation: representations){
                count += addTupleToQueue(stormTaskTuple, fileServiceClient, representation);
            }
        }
        return count;
    }
}
