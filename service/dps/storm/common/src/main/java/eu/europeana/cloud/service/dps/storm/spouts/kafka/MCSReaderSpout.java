package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.job.TaskExecutor;
//import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;
import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

/**
 * Created by Tarek on 5/18/2018.
 */
public class MCSReaderSpout extends CustomKafkaSpout {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(MCSReaderSpout.class);

    private SpoutOutputCollector collector;

    TaskDownloader taskDownloader;
    private String mcsClientURL;

    public MCSReaderSpout(KafkaSpoutConfig spoutConf, String hosts, int port, String keyspaceName,
                          String userName, String password, String mcsClientURL) {
        super(spoutConf, hosts, port, keyspaceName, userName, password);
        this.mcsClientURL = mcsClientURL;
    }

    public MCSReaderSpout(KafkaSpoutConfig spoutConf) {
        super(spoutConf);
        taskDownloader = new TaskDownloader();
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
            stormTaskTuple = taskDownloader.getTupleWithFileURL();
            if (stormTaskTuple != null) {
                collector.emit(stormTaskTuple.toStormTuple());
            }
        } catch (Exception e) {
            LOGGER.error("StaticDpsTaskSpout error: " + e.getMessage(), e);
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

    public TaskDownloader getTaskDownloader() {
        return taskDownloader;
    }

    public final class TaskDownloader extends Thread implements TaskQueueFiller {
        private static final int MAX_SIZE = 100;
        private static final int INTERNAL_THREADS_NUMBER = 10;

        private ArrayBlockingQueue<DpsTask> taskQueue = new ArrayBlockingQueue<>(MAX_SIZE);
        private ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(MAX_SIZE * INTERNAL_THREADS_NUMBER);
        private DpsTask currentDpsTask;
        private ExecutorService executorService;

        public TaskDownloader() {
            executorService = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
            start();
        }


        public StormTaskTuple getTupleWithFileURL() {
            return tuplesWithFileUrls.poll();
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
                        startProgressing(currentDpsTask);
                        OAIPMHHarvestingDetails oaipmhHarvestingDetails = currentDpsTask.getHarvestingDetails();
                        if (oaipmhHarvestingDetails == null)
                            oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
                        String stream = getStream(currentDpsTask);
                        if (stream.equals(FILE_URLS.name())) {
                            stormTaskTuple = new StormTaskTuple(
                                    currentDpsTask.getTaskId(),
                                    currentDpsTask.getTaskName(),
                                    null, null, currentDpsTask.getParameters(), currentDpsTask.getOutputRevision(), oaipmhHarvestingDetails);
                            List<String> files = currentDpsTask.getDataEntry(InputDataType.valueOf(stream));
                            for (String file : files) {
                                StormTaskTuple fileTuple = new Cloner().deepClone(stormTaskTuple);
                                fileTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, file);
                                tuplesWithFileUrls.put(fileTuple);
                            }
                        } else { // For data Sets
                            executorService.submit(
                                    new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                                            tuplesWithFileUrls, mcsClientURL, stream, currentDpsTask));
                        }
                    } else {
                        LOGGER.info("Skipping DROPPED task {}", currentDpsTask.getTaskId());
                    }
                } catch (Exception e) {
                    LOGGER.error("StaticDpsTaskSpout error: " + e.getMessage(), e);
                    if (stormTaskTuple != null)
                        cassandraTaskInfoDAO.dropTask(stormTaskTuple.getTaskId(), "The task was dropped because " + e.getMessage(), TaskState.DROPPED.toString());
                }
            }
        }

        private DpsTask getCurrentDpsTask() {
            return currentDpsTask;
        }

        private void startProgressing(DpsTask dpsTask) {
            LOGGER.info("Start progressing for Task with id {}", dpsTask.getTaskId());
            cassandraTaskInfoDAO.updateTask(dpsTask.getTaskId(), "", String.valueOf(TaskState.CURRENTLY_PROCESSING), new Date());
        }

        private String getStream(DpsTask task) {
            if (task.getInputData().get(FILE_URLS) != null)
                return FILE_URLS.name();
            return DATASET_URLS.name();
        }

        public ArrayBlockingQueue<DpsTask> getTaskQueue() {
            return taskQueue;
        }
    }

}