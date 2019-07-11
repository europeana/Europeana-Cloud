package eu.europeana.cloud.service.dps.storm.spouts.kafka.job;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;
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
import eu.europeana.cloud.service.dps.storm.spouts.kafka.QueueFiller;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.QueueFillerForLatestRevisionJob;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

public class TaskExecutor implements Callable<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);
    private static volatile TaskStatusChecker taskStatusChecker = TaskStatusChecker.getTaskStatusChecker();

    private SpoutOutputCollector collector;
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;
    private ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls;

    private String stream;
    private DpsTask dpsTask;

    private String mcsClientURL;

    public TaskExecutor() {
        this(null, null, null, null, null, null);
    }

    public TaskExecutor(SpoutOutputCollector collector, CassandraTaskInfoDAO cassandraTaskInfoDAO, ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls, String stream, DpsTask dpsTask, String mcsClientURL) {
        this.collector = collector;
        this.cassandraTaskInfoDAO = cassandraTaskInfoDAO;
        this.tuplesWithFileUrls = tuplesWithFileUrls;

        this.stream = stream;
        this.dpsTask = dpsTask;

        this.mcsClientURL = mcsClientURL;
    }

    @Override
    public Void call() throws Exception {
        execute();
        return null;
    }

    private void execute() throws Exception {

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

        int expectedSize = 0;
        try {
            for (String dataSetUrl : dataSets) {
                try {
                    final UrlParser urlParser = new UrlParser(dataSetUrl);
                    if (urlParser.isUrlToDataset()) {
                        if (revisionName != null && revisionProvider != null) {
                            String revisionTimestamp = dpsTask.getParameter(PluginParameterKeys.REVISION_TIMESTAMP);
                            if (revisionTimestamp != null) {
                                if (stormTaskTuple.getParameter(PluginParameterKeys.SAMPLE_SIZE) == null)
                                    expectedSize += handleExactRevisions(stormTaskTuple, dataSetServiceClient, recordServiceClient, fileClient, representationName, revisionName, revisionProvider, revisionTimestamp, urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
                                else
                                    expectedSize += handlePartialSizeExactRevisions(stormTaskTuple, dataSetServiceClient, recordServiceClient, fileClient, representationName, revisionName, revisionProvider, revisionTimestamp, urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS), Integer.parseInt(stormTaskTuple.getParameter(PluginParameterKeys.SAMPLE_SIZE)));

                            } else {
                                if (stormTaskTuple.getParameter(PluginParameterKeys.SAMPLE_SIZE) == null)
                                    expectedSize += handleLatestRevisions(stormTaskTuple, dataSetServiceClient, recordServiceClient, fileClient, representationName, revisionName, revisionProvider, urlParser.getPart(UrlPart.DATA_SETS), urlParser.getPart(UrlPart.DATA_PROVIDERS));
                                else
                                    expectedSize += handlePartialSizeForLatestRevisions(stormTaskTuple, dataSetServiceClient, recordServiceClient, fileClient, representationName, revisionName, revisionProvider, urlParser.getPart(UrlPart.DATA_SETS), urlParser.getPart(UrlPart.DATA_PROVIDERS), Integer.parseInt(stormTaskTuple.getParameter(PluginParameterKeys.SAMPLE_SIZE)));
                            }
                        } else {
                            QueueFiller queueFiller = new QueueFiller(taskStatusChecker, collector, tuplesWithFileUrls);
                            RepresentationIterator iterator = dataSetServiceClient.getRepresentationIterator(urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
                            while (iterator.hasNext() && !taskStatusChecker.hasKillFlag(dpsTask.getTaskId())) {
                                expectedSize += queueFiller.addTupleToQueue(stormTaskTuple, fileClient, iterator.next());
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
            cassandraTaskInfoDAO.setUpdateExpectedSize(dpsTask.getTaskId(), expectedSize);
        } finally {
            fileClient.close();
            dataSetServiceClient.close();
            recordServiceClient.close();
            if (expectedSize == 0)
                cassandraTaskInfoDAO.dropTask(dpsTask.getTaskId(), "The task was dropped because it is empty", TaskState.DROPPED.toString());

        }
    }

    private int handleExactRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        return -1;
    }

    private int handlePartialSizeExactRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName, int maxRecordsCount) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        return -1;
    }

    private int handleLatestRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileServiceClient, String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        return -1;
    }

    private int handlePartialSizeForLatestRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileServiceClient, String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider, int
            maxRecordsCount) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        return -1;
    }

    private void emitErrorNotification(long taskId, String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.ERROR, message, additionalInformations);
        collector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

}
