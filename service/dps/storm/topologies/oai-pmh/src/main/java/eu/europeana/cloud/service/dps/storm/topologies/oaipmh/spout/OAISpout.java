package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CollectorWrapper;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CustomKafkaSpout;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.TaskQueueFiller;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.job.IdentifierHarvester;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

/**
 * Created by Tarek on 4/30/2018.
 */
public class OAISpout extends CustomKafkaSpout {

    private SpoutOutputCollector collector;
    private static final Logger LOGGER = LoggerFactory.getLogger(OAISpout.class);
    private TaskDownloader taskDownloader;

    public OAISpout(SpoutConfig spoutConf, String hosts, int port, String keyspaceName,
                    String userName, String password) {
        super(spoutConf, hosts, port, keyspaceName, userName, password);

    }


    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        taskDownloader = new TaskDownloader();
        super.open(conf, context, new CollectorWrapper(collector, taskDownloader));
    }

    @Override
    public void nextTuple() {
        StormTaskTuple stormTaskTuple = null;
        try {
            super.nextTuple();
            stormTaskTuple = taskDownloader.getTupleWithOAIIdentifier();
            if (stormTaskTuple != null) {
                collector.emit(stormTaskTuple.toStormTuple());
            }
        } catch (Exception e) {
            LOGGER.error("Spout error: {}", e.getMessage());
            if (stormTaskTuple != null)
                cassandraTaskInfoDAO.dropTask(stormTaskTuple.getTaskId(), "The task was dropped because " + e.getMessage(), TaskState.DROPPED.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(StormTaskTuple.getFields());
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }

    @Override
    public void deactivate() {
        LOGGER.info("Deactivate method was executed");
        deactivateWaitingTasks();
        deactivateCurrentTask();
        LOGGER.info("Deactivate method was finished");
    }

    private void deactivateWaitingTasks() {
        DpsTask dpsTask;
        while ((dpsTask = taskDownloader.taskQueue.poll()) != null)
            cassandraTaskInfoDAO.dropTask(dpsTask.getTaskId(), "The task was dropped because of redeployment", TaskState.DROPPED.toString());
    }

    private void deactivateCurrentTask() {
        DpsTask currentDpsTask = taskDownloader.getCurrentDpsTask();
        if (currentDpsTask != null) {
            cassandraTaskInfoDAO.dropTask(currentDpsTask.getTaskId(), "The task was dropped because of redeployment", TaskState.DROPPED.toString());
        }
    }


    final class TaskDownloader extends Thread implements TaskQueueFiller {
        private static final int MAX_SIZE = 100;
        private static final int INTERNAL_THREADS_NUMBER = 10;
        ArrayBlockingQueue<DpsTask> taskQueue = new ArrayBlockingQueue<>(MAX_SIZE);
        private ArrayBlockingQueue<StormTaskTuple> oaiIdentifiers = new ArrayBlockingQueue<>(MAX_SIZE * INTERNAL_THREADS_NUMBER);
        ExecutorService executorService;
        private DpsTask currentDpsTask;


        public TaskDownloader() {
            start();
            executorService = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
        }

        public StormTaskTuple getTupleWithOAIIdentifier() {
            return oaiIdentifiers.poll();
        }

        public void addNewTask(DpsTask dpsTask) {
            try {
                taskQueue.put(dpsTask);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void run() {
            StormTaskTuple stormTaskTuple = null;
            while (true) {
                try {
                    currentDpsTask = taskQueue.take();
                    if (!taskStatusChecker.hasKillFlag(currentDpsTask.getTaskId())) {
                        OAIPMHHarvestingDetails oaipmhHarvestingDetails = currentDpsTask.getHarvestingDetails();
                        if (oaipmhHarvestingDetails == null)
                            oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
                        stormTaskTuple = new StormTaskTuple(
                                currentDpsTask.getTaskId(),
                                currentDpsTask.getTaskName(),
                                currentDpsTask.getDataEntry(InputDataType.REPOSITORY_URLS).get(0), null, currentDpsTask.getParameters(), currentDpsTask.getOutputRevision(), oaipmhHarvestingDetails);
                        executorService.submit(new IdentifierHarvester(stormTaskTuple, cassandraTaskInfoDAO, oaiIdentifiers, taskStatusChecker));
                    } else {
                        LOGGER.info("Skipping DROPPED task {}", currentDpsTask.getTaskId());
                    }
                } catch (Exception e) {
                    LOGGER.error("StaticDpsTaskSpout error: {}", e.getMessage());
                    if (stormTaskTuple != null)
                        cassandraTaskInfoDAO.dropTask(stormTaskTuple.getTaskId(), "The task was dropped because " + e.getMessage(), TaskState.DROPPED.toString());
                }
            }
        }

        DpsTask getCurrentDpsTask() {
            return currentDpsTask;
        }
    }
}


