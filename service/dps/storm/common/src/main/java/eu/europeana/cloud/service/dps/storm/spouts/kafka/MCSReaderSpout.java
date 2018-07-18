package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.utils.TaskSpoutInfo;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;

/**
 * Created by Tarek on 5/18/2018.
 */
public class MCSReaderSpout extends CustomKafkaSpout {
    private static final int RECORDS_COUNT_TO_WAIT = 200;
    private static final int WAITING_IN_MILLIS = 1500;

    private SpoutOutputCollector collector;
    private static final Logger LOGGER = LoggerFactory.getLogger(MCSReaderSpout.class);

    private transient ConcurrentHashMap<Long, TaskSpoutInfo> cache;
    // default number of retries
    private static final int DEFAULT_RETRIES = 10;
    private static final int SLEEP_TIME = 5000;
    private static final String NOTIFICATION_STREAM_NAME = "NotificationStream";

    private String mcsClientURL;

    MCSReaderSpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }

    public MCSReaderSpout(SpoutConfig spoutConf, String hosts, int port, String keyspaceName,
                          String userName, String password, String mcsClientURL) {
        super(spoutConf, hosts, port, keyspaceName, userName, password);
        this.mcsClientURL = mcsClientURL;

    }


    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        cache = new ConcurrentHashMap<>(50);
        super.open(conf, context, new CollectorWrapper(collector));
    }

    @Override
    public void nextTuple() {
        DpsTask dpsTask = null;
        try {
            super.nextTuple();
            for (long taskId : cache.keySet()) {
                TaskSpoutInfo currentTask = cache.get(taskId);
                if (!currentTask.isStarted()) {
                    LOGGER.info("Start progressing for Task with id {}", currentTask.getDpsTask().getTaskId());
                    startProgress(currentTask);
                    dpsTask = currentTask.getDpsTask();
                    String stream = getStream(dpsTask);

                    if (stream.equals(FILE_URLS.name())) {
                        StormTaskTuple stormTaskTuple = new StormTaskTuple(
                                dpsTask.getTaskId(),
                                dpsTask.getTaskName(),
                                null, null, dpsTask.getParameters(), dpsTask.getOutputRevision(), new OAIPMHHarvestingDetails());
                        List<String> files = dpsTask.getDataEntry(InputDataType.valueOf(stream));
                        for (String file : files) {
                            StormTaskTuple fileTuple = new Cloner().deepClone(stormTaskTuple);
                            fileTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, file);
                            collector.emit(fileTuple.toStormTuple());
                        }
                    } else // For data Sets
                        execute(stream, dpsTask);
                    cache.remove(taskId);
                }
            }

        } catch (Exception e) {
            LOGGER.error("StaticDpsTaskSpout error: {}", e.getMessage());
            if (dpsTask != null)
                cassandraTaskInfoDAO.dropTask(dpsTask.getTaskId(), "The task was dropped because " + e.getMessage(), TaskState.DROPPED.toString());
        }
    }

    private String getStream(DpsTask task) {
        if (task.getInputData().get(FILE_URLS) != null)
            return FILE_URLS.name();
        return DATASET_URLS.name();
    }

    private void startProgress(TaskSpoutInfo taskInfo) {
        taskInfo.startTheTask();
        DpsTask task = taskInfo.getDpsTask();
        cassandraTaskInfoDAO.updateTask(task.getTaskId(), "", String.valueOf(TaskState.CURRENTLY_PROCESSING), new Date());

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(StormTaskTuple.getFields());
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }


    public void execute(String stream, DpsTask dpsTask) throws MCSException, DriverException {

        final List<String> dataSets = dpsTask.getDataEntry(InputDataType.valueOf(stream));
        final String representationName = dpsTask.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        dpsTask.getParameters().remove(PluginParameterKeys.REPRESENTATION_NAME);

        final String revisionName = dpsTask.getParameter(PluginParameterKeys.REVISION_NAME);
        dpsTask.getParameters().remove(PluginParameterKeys.REVISION_NAME);

        final String revisionProvider = dpsTask.getParameter(PluginParameterKeys.REVISION_PROVIDER);
        dpsTask.getParameters().remove(PluginParameterKeys.REVISION_PROVIDER);

        final String authorizationHeader = dpsTask.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        DataSetServiceClient dataSetServiceClient = new DataSetServiceClient(mcsClientURL);
        dataSetServiceClient.useAuthorizationHeader(authorizationHeader);

        RecordServiceClient recordServiceClient = new RecordServiceClient(mcsClientURL);
        recordServiceClient.useAuthorizationHeader(authorizationHeader);

        FileServiceClient fileClient = new FileServiceClient(mcsClientURL);
        fileClient.useAuthorizationHeader(authorizationHeader);

        final StormTaskTuple stormTaskTuple = new StormTaskTuple(
                dpsTask.getTaskId(),
                dpsTask.getTaskName(),
                null, null, dpsTask.getParameters(), dpsTask.getOutputRevision(), new OAIPMHHarvestingDetails());

        try {
            for (String dataSetUrl : dataSets) {
                try {
                    final UrlParser urlParser = new UrlParser(dataSetUrl);
                    if (urlParser.isUrlToDataset()) {
                        if (revisionName != null && revisionProvider != null) {
                            String revisionTimestamp = dpsTask.getParameter(PluginParameterKeys.REVISION_TIMESTAMP);
                            if (revisionTimestamp != null) {
                                handleExactRevisions(stormTaskTuple, dataSetServiceClient, recordServiceClient, fileClient, representationName, revisionName, revisionProvider, revisionTimestamp, urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
                            } else {
                                handleLatestRevisions(stormTaskTuple, dataSetServiceClient, recordServiceClient, fileClient, representationName, revisionName, revisionProvider, urlParser.getPart(UrlPart.DATA_SETS), urlParser.getPart(UrlPart.DATA_PROVIDERS));
                            }
                        } else {
                            RepresentationIterator iterator = dataSetServiceClient.getRepresentationIterator(urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
                            while (iterator.hasNext() && !taskStatusChecker.hasKillFlag(dpsTask.getTaskId())) {
                                final Representation representation = iterator.next();
                                emitFilesFromRepresentation(stormTaskTuple, fileClient, representation);
                            }
                        }
                    } else {
                        LOGGER.warn("dataSet url is not formulated correctly {}", dataSetUrl);
                        emitErrorNotification(dpsTask.getTaskId(), dataSetUrl, "dataSet url is not formulated correctly", "");
                    }
                } catch (MalformedURLException ex) {
                    LOGGER.error("MCSReaderSpout error, Error while parsing DataSet URL : {}", ex.getMessage());
                    emitErrorNotification(dpsTask.getTaskId(), dataSetUrl, ex.getMessage(), dpsTask.getParameters().toString());
                }
            }
            cassandraTaskInfoDAO.setUpdateExpectedSize(dpsTask.getTaskId(), cache.get(dpsTask.getTaskId()).getFileCount());
        } finally {
            fileClient.close();
            dataSetServiceClient.close();
            recordServiceClient.close();
            if (cache.get(dpsTask.getTaskId()).getFileCount() == 0)
                cassandraTaskInfoDAO.dropTask(dpsTask.getTaskId(), "The task was dropped because it is empty", TaskState.DROPPED.toString());

        }
    }

    private void handleLatestRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileServiceClient, String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider) throws MCSException, DriverException {

        String startFrom = null;
        final long taskId = stormTaskTuple.getTaskId();
        do {
            final ResultSlice<CloudIdAndTimestampResponse> resultSlice = getLatestDataSetCloudIdByRepresentationAndRevisionChunk(dataSetServiceClient, representationName, revisionName, revisionProvider, datasetName, datasetProvider, startFrom);
            final List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = resultSlice.getResults();
            for (CloudIdAndTimestampResponse cloudIdAndTimestampResponse : cloudIdAndTimestampResponseList) {
                if (!taskStatusChecker.hasKillFlag(taskId)) {
                    final String responseCloudId = cloudIdAndTimestampResponse.getCloudId();
                    final RepresentationRevisionResponse representationRevisionResponse = getRepresentationRevision(recordServiceClient, representationName, revisionName, revisionProvider, DateHelper.getUTCDateString(cloudIdAndTimestampResponse.getRevisionTimestamp()), responseCloudId);
                    final Representation representation = getRepresentation(recordServiceClient, representationName, responseCloudId, representationRevisionResponse);
                    emitFilesFromRepresentation(stormTaskTuple, fileServiceClient, representation);
                } else
                    break;
            }
            startFrom = resultSlice.getNextSlice();
        }
        while (startFrom != null && !taskStatusChecker.hasKillFlag(taskId));
    }

    private void emitFilesFromRepresentation(StormTaskTuple stormTaskTuple, FileServiceClient fileServiceClient, Representation representation) {
        final long taskId = stormTaskTuple.getTaskId();
        if (representation != null) {
            for (eu.europeana.cloud.common.model.File file : representation.getFiles()) {
                String fileUrl = "";
                if (!taskStatusChecker.hasKillFlag(taskId)) {
                    try {
                        fileUrl = fileServiceClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName()).toString();
                        final StormTaskTuple fileTuple = buildNextStormTuple(stormTaskTuple, fileUrl);
                        cache.get(taskId).inc();
                        if (cache.get(taskId).getFileCount() % RECORDS_COUNT_TO_WAIT == 0)
                            waitForSpecificTime(WAITING_IN_MILLIS);
                        collector.emit(fileTuple.toStormTuple());
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
    }


    private ResultSlice<CloudIdAndTimestampResponse> getLatestDataSetCloudIdByRepresentationAndRevisionChunk(DataSetServiceClient dataSetServiceClient, String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider, String startFrom) throws MCSException, DriverException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                ResultSlice<CloudIdAndTimestampResponse> resultSlice = dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(datasetName, datasetProvider, revisionProvider, revisionName, representationName, false, startFrom);
                if (resultSlice == null || resultSlice.getResults() == null) {
                    throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
                }
                return resultSlice;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting slice of  latest cloud Id from data set.Retries Left{} ", retries);
                    waitForSpecificTime(SLEEP_TIME);
                } else {
                    LOGGER.error("Error while getting slice of latest cloud Id from data set.");
                    throw e;
                }
            }
        }
    }


    private void handleExactRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName) throws MCSException, DriverException {
        String startFrom = null;
        long taskId = stormTaskTuple.getTaskId();
        do {
            ResultSlice<CloudTagsResponse> resultSlice = getDataSetRevisionsChunk(dataSetServiceClient, representationName, revisionName, revisionProvider, revisionTimestamp, datasetProvider, datasetName, startFrom);
            List<CloudTagsResponse> cloudTagsResponses = resultSlice.getResults();
            for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
                if (!taskStatusChecker.hasKillFlag(taskId)) {
                    String responseCloudId = cloudTagsResponse.getCloudId();
                    RepresentationRevisionResponse representationRevisionResponse = getRepresentationRevision(recordServiceClient, representationName, revisionName, revisionProvider, revisionTimestamp, responseCloudId);
                    Representation representation = getRepresentation(recordServiceClient, representationName, responseCloudId, representationRevisionResponse);
                    emitFilesFromRepresentation(stormTaskTuple, fileClient, representation);
                } else
                    break;
            }
            startFrom = resultSlice.getNextSlice();
        }
        while (startFrom != null && !taskStatusChecker.hasKillFlag(taskId));
    }

    private Representation getRepresentation(RecordServiceClient recordServiceClient, String representationName, String responseCloudId, RepresentationRevisionResponse representationRevisionResponse) throws MCSException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return recordServiceClient.getRepresentation(responseCloudId, representationName, representationRevisionResponse.getVersion());
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting Representation. Retries left{}", retries);
                    waitForSpecificTime(SLEEP_TIME);
                } else {
                    LOGGER.error("Error while getting Representation.");
                    throw e;
                }
            }
        }
    }

    private RepresentationRevisionResponse getRepresentationRevision(RecordServiceClient recordServiceClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String responseCloudId) throws MCSException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return recordServiceClient.getRepresentationRevision(responseCloudId, representationName, revisionName, revisionProvider, revisionTimestamp);
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


    private ResultSlice<CloudTagsResponse> getDataSetRevisionsChunk(DataSetServiceClient dataSetServiceClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName, String startFrom) throws MCSException, DriverException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                ResultSlice<CloudTagsResponse> resultSlice = dataSetServiceClient.getDataSetRevisionsChunk(datasetProvider, datasetName, representationName,
                        revisionName, revisionProvider, revisionTimestamp, startFrom, null);
                if (resultSlice == null || resultSlice.getResults() == null) {
                    throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
                }

                return resultSlice;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting Revisions from data set.Retries Left{} ", retries);
                    waitForSpecificTime(SLEEP_TIME);
                } else {
                    LOGGER.error("Error while getting Revisions from data set.");
                    throw e;
                }
            }
        }
    }


    private StormTaskTuple buildNextStormTuple(StormTaskTuple stormTaskTuple, String fileUrl) {
        StormTaskTuple fileTuple = new Cloner().deepClone(stormTaskTuple);
        fileTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, fileUrl);
        return fileTuple;
    }

    private void waitForSpecificTime(int milliSecond) {
        try {
            Thread.sleep(milliSecond);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }

    private void emitErrorNotification(long taskId, String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.ERROR, message, additionalInformations);
        collector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }


    private class CollectorWrapper extends SpoutOutputCollector {

        CollectorWrapper(ISpoutOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            try {
                DpsTask dpsTask = new ObjectMapper().readValue((String) tuple.get(0), DpsTask.class);
                if (dpsTask != null) {
                    long taskId = dpsTask.getTaskId();
                    cache.putIfAbsent(taskId, new TaskSpoutInfo(dpsTask));
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }

            return Collections.emptyList();
        }
    }
}