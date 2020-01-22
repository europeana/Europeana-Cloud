package eu.europeana.cloud.service.dps.storm.spouts.kafka.job;

import com.google.common.base.Throwables;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.response.CloudTagsResponse;
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
import eu.europeana.cloud.service.dps.storm.spouts.kafka.QueueFillerForSpecificRevisionJob;
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
    private static final int INTERNAL_THREADS_NUMBER = 10;
    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;
    private static final int MAX_BATCH_SIZE = 100;


    private TaskStatusChecker taskStatusChecker;

    private SpoutOutputCollector collector;
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;
    private ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls;

    private String mcsClientURL;

    private String stream;
    private DpsTask dpsTask;

    public TaskExecutor(SpoutOutputCollector collector, TaskStatusChecker taskStatusChecker, CassandraTaskInfoDAO cassandraTaskInfoDAO,
                        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls, String mcsClientURL, String stream, DpsTask dpsTask) {
        this.collector = collector;
        this.taskStatusChecker = taskStatusChecker;
        this.cassandraTaskInfoDAO = cassandraTaskInfoDAO;
        this.tuplesWithFileUrls = tuplesWithFileUrls;

        this.mcsClientURL = mcsClientURL;

        this.stream = stream;
        this.dpsTask = dpsTask;
    }

    @Override
    public Void call() {
        try {
            execute();
        } catch (Exception e) {
            cassandraTaskInfoDAO.dropTask(dpsTask.getTaskId(), "The task was dropped because of " + e.getMessage() + ". The full exception is" + Throwables.getStackTraceAsString(e), TaskState.DROPPED.toString());
        }
        return null;
    }

    void execute() throws Exception {
        final List<String> dataSets = dpsTask.getDataEntry(InputDataType.valueOf(stream));
        final String representationName = dpsTask.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        dpsTask.getParameters().remove(PluginParameterKeys.REPRESENTATION_NAME);

        final String revisionName = dpsTask.getParameter(PluginParameterKeys.REVISION_NAME);
        dpsTask.getParameters().remove(PluginParameterKeys.REVISION_NAME);

        final String revisionProvider = dpsTask.getParameter(PluginParameterKeys.REVISION_PROVIDER);
        dpsTask.getParameters().remove(PluginParameterKeys.REVISION_PROVIDER);


        final String authorizationHeader = dpsTask.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);

        DataSetServiceClient dataSetServiceClient = new DataSetServiceClient(mcsClientURL, authorizationHeader);

        RecordServiceClient recordServiceClient = new RecordServiceClient(mcsClientURL, authorizationHeader);

        FileServiceClient fileClient = new FileServiceClient(mcsClientURL, authorizationHeader);

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


    private int handleLatestRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileServiceClient, String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        int count = 0;
        String startFrom = null;
        final long taskId = stormTaskTuple.getTaskId();
        ExecutorService executorService = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
        Set<Future<Integer>> futures = new HashSet<>(INTERNAL_THREADS_NUMBER);
        do {
            final ResultSlice<CloudIdAndTimestampResponse> resultSlice = getLatestDataSetCloudIdByRepresentationAndRevisionChunk(dataSetServiceClient, representationName, revisionName, revisionProvider, datasetName, datasetProvider, startFrom);
            final List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = resultSlice.getResults();
            Future<Integer> job = executorService.submit(new QueueFillerForLatestRevisionJob(fileServiceClient, recordServiceClient, collector, taskStatusChecker, tuplesWithFileUrls, stormTaskTuple, representationName, revisionName, revisionProvider, cloudIdAndTimestampResponseList));
            futures.add(job);
            if (futures.size() == INTERNAL_THREADS_NUMBER) {
                count += getCountAndWait(futures);
            }
            startFrom = resultSlice.getNextSlice();
        }
        while (startFrom != null && !taskStatusChecker.hasKillFlag(taskId));

        if (futures.size() > 0)
            count += getCountAndWait(futures);
        executorService.shutdown();
        return count;
    }

    private int handlePartialSizeForLatestRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileServiceClient, String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider, int
            maxRecordsCount) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        int count = 0;
        String startFrom = null;
        final long taskId = stormTaskTuple.getTaskId();
        ExecutorService executorService = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
        Set<Future<Integer>> futures = new HashSet<>(INTERNAL_THREADS_NUMBER);
        int total = 0;
        do {

            final ResultSlice<CloudIdAndTimestampResponse> resultSlice = getLatestDataSetCloudIdByRepresentationAndRevisionChunk(dataSetServiceClient, representationName, revisionName, revisionProvider, datasetName, datasetProvider, startFrom);
            List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = resultSlice.getResults();
            total += cloudIdAndTimestampResponseList.size();
            if (total > maxRecordsCount)
                if (total - maxRecordsCount < MAX_BATCH_SIZE)
                    cloudIdAndTimestampResponseList = cloudIdAndTimestampResponseList.subList(0, maxRecordsCount % MAX_BATCH_SIZE);
                else
                    break;
            Future<Integer> job = executorService.submit(new QueueFillerForLatestRevisionJob(fileServiceClient, recordServiceClient, collector, taskStatusChecker, tuplesWithFileUrls, stormTaskTuple, representationName, revisionName, revisionProvider, cloudIdAndTimestampResponseList));
            futures.add(job);
            if (futures.size() == INTERNAL_THREADS_NUMBER) {
                count += getCountAndWait(futures);
            }
            startFrom = resultSlice.getNextSlice();
        }
        while (startFrom != null && !taskStatusChecker.hasKillFlag(taskId));

        if (futures.size() > 0)
            count += getCountAndWait(futures);
        executorService.shutdown();
        return count;
    }


    private int handleExactRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        int count = 0;
        String startFrom = null;
        long taskId = stormTaskTuple.getTaskId();
        ExecutorService executorService = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
        Set<Future<Integer>> futures = new HashSet<>(INTERNAL_THREADS_NUMBER);

        do {
            ResultSlice<CloudTagsResponse> resultSlice = getDataSetRevisionsChunk(dataSetServiceClient, representationName, revisionName, revisionProvider, revisionTimestamp, datasetProvider, datasetName, startFrom);
            List<CloudTagsResponse> cloudTagsResponses = resultSlice.getResults();
            Future<Integer> job = executorService.submit(new QueueFillerForSpecificRevisionJob(fileClient, recordServiceClient, collector, taskStatusChecker, tuplesWithFileUrls, stormTaskTuple, representationName, revisionName, revisionProvider, revisionTimestamp, cloudTagsResponses));
            futures.add(job);
            if (futures.size() == INTERNAL_THREADS_NUMBER) {
                count += getCountAndWait(futures);
            }
            startFrom = resultSlice.getNextSlice();
        }
        while (startFrom != null && !taskStatusChecker.hasKillFlag(taskId));

        if (futures.size() > 0)
            count += getCountAndWait(futures);
        executorService.shutdown();
        return count;
    }

    private int handlePartialSizeExactRevisions(StormTaskTuple stormTaskTuple, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, FileServiceClient fileClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName, int maxRecordsCount) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        int count = 0;
        String startFrom = null;
        long taskId = stormTaskTuple.getTaskId();
        ExecutorService executorService = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
        Set<Future<Integer>> futures = new HashSet<>(INTERNAL_THREADS_NUMBER);
        int total = 0;
        do {
            ResultSlice<CloudTagsResponse> resultSlice = getDataSetRevisionsChunk(dataSetServiceClient, representationName, revisionName, revisionProvider, revisionTimestamp, datasetProvider, datasetName, startFrom);
            List<CloudTagsResponse> cloudTagsResponses = resultSlice.getResults();
            total += cloudTagsResponses.size();
            if (total > maxRecordsCount)
                if (total - maxRecordsCount < MAX_BATCH_SIZE)
                    cloudTagsResponses = cloudTagsResponses.subList(0, maxRecordsCount % MAX_BATCH_SIZE);
                else
                    break;
            Future<Integer> job = executorService.submit(new QueueFillerForSpecificRevisionJob(fileClient, recordServiceClient, collector, taskStatusChecker, tuplesWithFileUrls, stormTaskTuple, representationName, revisionName, revisionProvider, revisionTimestamp, cloudTagsResponses));
            futures.add(job);
            if (futures.size() == INTERNAL_THREADS_NUMBER) {
                count += getCountAndWait(futures);
            }
            startFrom = resultSlice.getNextSlice();
        }
        while (startFrom != null && !taskStatusChecker.hasKillFlag(taskId));

        if (futures.size() > 0)
            count += getCountAndWait(futures);
        executorService.shutdown();
        return count;
    }

    private int getCountAndWait(Set<Future<Integer>> futures) throws InterruptedException, ExecutionException {
        int count = 0;
        for (Future<Integer> future : futures) {
            count += future.get();
        }
        futures.clear();
        return count;
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
                resource, RecordState.ERROR, message, additionalInformations);
        collector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

}
