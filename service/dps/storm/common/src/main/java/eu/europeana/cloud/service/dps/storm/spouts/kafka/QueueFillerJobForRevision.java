package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

/**
 * Created by Tarek on 9/14/2018.
 */
public abstract class QueueFillerJobForRevision extends QueueFiller implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueFillerJobForRevision.class);

    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;
    protected RecordServiceClient recordServiceClient;
    protected FileServiceClient fileServiceClient;
    protected String representationName;
    protected StormTaskTuple stormTaskTuple;
    protected String revisionName;
    protected String revisionProvider;


    public QueueFillerJobForRevision(RecordServiceClient recordServiceClient, /*FileServiceClient fileServiceClient,*/ String representationName, StormTaskTuple stormTaskTuple, String revisionName, String revisionProvider, TaskStatusChecker taskStatusChecker, SpoutOutputCollector collector, ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls) {
        super(taskStatusChecker, collector, tuplesWithFileUrls);
        this.recordServiceClient = recordServiceClient;
        this.representationName = representationName;
        this.stormTaskTuple = stormTaskTuple;
        this.revisionName = revisionName;
        this.revisionProvider = revisionProvider;

    }


    protected List<Representation> getRepresentationByRevision(RecordServiceClient recordServiceClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String responseCloudId) throws MCSException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return recordServiceClient.getRepresentationsByRevision(responseCloudId, representationName, revisionName, revisionProvider, revisionTimestamp);
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

    private void waitForSpecificTime(int milliSecond) {
        try {
            Thread.sleep(milliSecond);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }

    abstract int fillTheQueue() throws MCSException;

    @Override
    public Integer call() throws Exception {
        return fillTheQueue();
    }
}
