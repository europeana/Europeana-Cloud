package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.Instant;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;

public class MCSTaskSubmiter {

    private static final int INTERNAL_THREADS_NUMBER = 10;
    private static final int MAX_BATCH_SIZE = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(MCSTaskSubmiter.class);

    private final TaskStatusChecker taskStatusChecker;

    private final CassandraTaskInfoDAO cassandraTaskInfoDAO;

    private final RecordExecutionSubmitService recordSubmitService;

    private final String topologyName;

    private final DpsTask task;

    private final String topicName;

    private final MCSReader reader;
    private ExecutorService executorService;

    public MCSTaskSubmiter(TaskStatusChecker taskStatusChecker, CassandraTaskInfoDAO cassandraTaskInfoDAO, RecordExecutionSubmitService recordSubmitService, String topologyName, DpsTask task, String topicName, MCSReader reader) {
        this.taskStatusChecker = taskStatusChecker;
        this.cassandraTaskInfoDAO = cassandraTaskInfoDAO;
        this.recordSubmitService = recordSubmitService;
        this.topologyName = topologyName;
        this.task = task;
        this.topicName = topicName;
        this.reader = reader;
    }

    public void execute() {

        reader.open();
        try {
            LOGGER.info("Sending task id={} to topology {} by kafka topic {}", task.getTaskId(), topologyName, topicName);

            checkIfTaskIsKilled();

            startProgressing();
            if (taskContainsFileUrls()) {
                executeForFilesList();
            } else {
                executeForDatasetList();
            }
        } catch (SubmitingTaskWasKilled e) {
            LOGGER.warn(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error("MCSTaskSubmiter error for taskId=" + task.getTaskId() + " error: " + e.getMessage(), e);
            cassandraTaskInfoDAO.dropTask(task.getTaskId(), "The task was dropped because " + e.getMessage(), TaskState.DROPPED.toString());
        } finally {
            shutdownExecutor();
            reader.close();
        }
    }

    private void executeForFilesList() {
        List<String> filesList = task.getDataEntry(FILE_URLS);
        for (String file : filesList) {
            submitRecord(file);
        }
        cassandraTaskInfoDAO.setUpdateExpectedSize(task.getTaskId(), filesList.size());
    }

    private void executeForDatasetList() throws Exception {
        int expectedSize = 0;
        try {
            for (String dataSetUrl : task.getDataEntry(InputDataType.DATASET_URLS)) {
                expectedSize += executeForOneDataSet(dataSetUrl);
            }
            cassandraTaskInfoDAO.setUpdateExpectedSize(task.getTaskId(), expectedSize);
        } finally {
            if (expectedSize == 0) {
                cassandraTaskInfoDAO.dropTask(task.getTaskId(), "The task was dropped because it is empty", TaskState.DROPPED.toString());
                LOGGER.warn("The task id={} was dropped because it is empty.", task.getTaskId());
            }
        }


    }

    private int executeForOneDataSet(String dataSetUrl) throws MCSException, InterruptedException, ExecutionException {
        try {
            UrlParser urlParser = new UrlParser(dataSetUrl);
            if (!urlParser.isUrlToDataset()) {
                throw new RuntimeException("DataSet URL is not formulated correctly: " + dataSetUrl);
            }

            int expectedSize = 0;
            if (getRevisionName() != null && getRevisionProvider() != null) {
                expectedSize += executeForRevision(urlParser.getPart(UrlPart.DATA_SETS), urlParser.getPart(UrlPart.DATA_PROVIDERS));
            } else {
                expectedSize += executeForEntireDataset(urlParser);

            }
            return expectedSize;

        } catch (MalformedURLException e) {
            throw new RuntimeException("MCSReaderSpout error, Error while parsing DataSet URL : \"" + dataSetUrl + "\"", e);
        }

    }

    private int executeForEntireDataset(UrlParser urlParser) {
        int expectedSize = 0;
        RepresentationIterator iterator = reader.getRepresentationsOfEntireDataset(urlParser);
        while (iterator.hasNext()) {
            checkIfTaskIsKilled();
            expectedSize += submitRecordsForAllFilesOfRepresentation(iterator.next());
        }
        return expectedSize;
    }

    private int executeForRevision(String datasetName, String datasetProvider) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        int maxRecordsCount = Optional.ofNullable(task.getParameter(PluginParameterKeys.SAMPLE_SIZE)).map(Integer::parseInt).orElse(Integer.MAX_VALUE);
        int count = 0;
        String startFrom = null;
        Set<Future<Integer>> futures = new HashSet<>(INTERNAL_THREADS_NUMBER);
        int total = 0;
        do {
            checkIfTaskIsKilled();
            ResultSlice<CloudIdAndTimestampResponse> slice = getCloudIdsChunk(datasetName, datasetProvider, startFrom);
            List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = slice.getResults();
            total += cloudIdAndTimestampResponseList.size();
            if (total > maxRecordsCount)
                if (total - maxRecordsCount < MAX_BATCH_SIZE) {
                    cloudIdAndTimestampResponseList = cloudIdAndTimestampResponseList.subList(0, maxRecordsCount % MAX_BATCH_SIZE);
                } else {
                    break;
                }

            final List<CloudIdAndTimestampResponse> finalCloudIdAndTimestampResponseList = cloudIdAndTimestampResponseList;
            futures.add(getExecutor().submit(() -> executeGettingFileUrlsForCloudIdList(finalCloudIdAndTimestampResponseList)));

            if (futures.size() >= INTERNAL_THREADS_NUMBER * MAX_BATCH_SIZE) {
                count += getCountAndWait(futures);
            }
            startFrom = slice.getNextSlice();
        }
        while (startFrom != null);

        if (futures.size() > 0)
            count += getCountAndWait(futures);

        return count;
    }

    private ResultSlice<CloudIdAndTimestampResponse> getCloudIdsChunk(String datasetName, String datasetProvider, String startFrom) throws MCSException {
        if (getRevisionTimestamp() != null) {
            ResultSlice<CloudTagsResponse> chunk = reader.getDataSetRevisionsChunk(getRepresentationName(), getRevisionName(), getRevisionProvider(), getRevisionTimestamp(), datasetProvider, datasetName, startFrom);
            return toCloudAndTimestampResponse(chunk, getRevisionTimestamp());
        } else {
            return reader.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(getRepresentationName(), getRevisionName(), getRevisionProvider(), datasetName, datasetProvider, startFrom);
        }
    }

    private Integer executeGettingFileUrlsForCloudIdList(List<CloudIdAndTimestampResponse> responseList) throws MCSException {
        int count = 0;
        for (CloudIdAndTimestampResponse response : responseList) {
            count += executeGettingFileUrlsForOneCloudId(response);
        }

        return count;
    }

    private int executeGettingFileUrlsForOneCloudId(CloudIdAndTimestampResponse response) throws MCSException {
        String revisionTimestamp = DateHelper.getUTCDateString(response.getRevisionTimestamp());
        int count = 0;
        List<Representation> representations = reader.getRepresentationsByRevision(getRepresentationName(), getRevisionName(), getRevisionProvider(), revisionTimestamp, response.getCloudId());
        for (Representation representation : representations) {
            count += submitRecordsForAllFilesOfRepresentation(representation);
        }
        return count;
    }

    private int submitRecordsForAllFilesOfRepresentation(Representation representation) {
        int count = 0;
        if (representation == null) {
            throw new RuntimeException("Problem while reading representation - representation is null.");
        }

        for (File file : representation.getFiles()) {
            checkIfTaskIsKilled();
            String fileUrl = reader.getFileUri(representation, file.getFileName());
            submitRecord(fileUrl);
            count++;

        }

        return count;
    }

    private ResultSlice<CloudIdAndTimestampResponse> toCloudAndTimestampResponse(ResultSlice<CloudTagsResponse> chunk, String revisionTimestamp) {
        return new ResultSlice<>(chunk.getNextSlice(),
                chunk.getResults()
                        .stream()
                        .map(response -> new CloudIdAndTimestampResponse(response.getCloudId(), Date.from(Instant.parse(revisionTimestamp))))
                        .collect(Collectors.toList()));
    }

    private void submitRecord(String fileUrl) {
        DpsRecord record = DpsRecord.builder().taskId(task.getTaskId()).metadataPrefix(getSchemaName()).inputData(fileUrl).build();
        recordSubmitService.submitRecord(record, topicName);
    }

    private void startProgressing() {
        LOGGER.info("Start progressing for Task with id {}", task.getTaskId());
        cassandraTaskInfoDAO.updateTask(task.getTaskId(), "", String.valueOf(TaskState.CURRENTLY_PROCESSING), new Date());
    }

    private boolean taskContainsFileUrls() {
        return task.getInputData().get(FILE_URLS) != null;
    }

    private int getCountAndWait(Set<Future<Integer>> futures) throws InterruptedException, ExecutionException {
        int count = 0;
        for (Future<Integer> future : futures) {
            count += future.get();
        }
        futures.clear();
        return count;
    }

    private void checkIfTaskIsKilled() {
        if (taskStatusChecker.hasKillFlag(task.getTaskId())) {
            throw new SubmitingTaskWasKilled(task);
        }
    }

    private ExecutorService getExecutor() {
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
        }
        return executorService;
    }

    private void shutdownExecutor() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private String getRevisionTimestamp() {
        return task.getParameter(PluginParameterKeys.REVISION_TIMESTAMP);
    }

    private String getRevisionProvider() {
        return task.getParameter(PluginParameterKeys.REVISION_PROVIDER);
    }

    private String getRevisionName() {
        return task.getParameter(PluginParameterKeys.REVISION_NAME);
    }

    private String getRepresentationName() {
        return task.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
    }

    private String getSchemaName() {
        return task.getParameter(PluginParameterKeys.SCHEMA_NAME);
    }



}
