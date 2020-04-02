package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.RecordState;
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
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskErrorsDAO;
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

    private final static Logger LOGGER = LoggerFactory.getLogger(MCSTaskSubmiter.class);

    private CassandraTaskErrorsDAO taskErrorDAO;

    private TaskStatusChecker taskStatusChecker;

    private CassandraTaskInfoDAO cassandraTaskInfoDAO;

    private RecordExecutionSubmitService recordSubmitService;

    private String topologyName;

    private DpsTask task;

    private String topicName;

    private MCSReader reader;

    public MCSTaskSubmiter(CassandraTaskErrorsDAO taskErrorDAO, TaskStatusChecker taskStatusChecker, CassandraTaskInfoDAO cassandraTaskInfoDAO, RecordExecutionSubmitService recordSubmitService, String topologyName, DpsTask task, String topicName, MCSReader reader) {
        this.taskErrorDAO = taskErrorDAO;
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
            LOGGER.info("PENDING TASK TO TOPOLOGY " + topologyName + " BY KAFKA TOPIC " + topicName);

            if (!taskStatusChecker.hasKillFlag(task.getTaskId())) {
                startProgressing();
                if (taskContainsFileUrls()) {
                    executeForFilesList();
                } else {
                    executeForDatasets();
                }
            } else {
                //TODO moze zdrobić drooped exception
                LOGGER.info("Skipping DROPPED task {}", task.getTaskId());
            }

        } catch (Exception e) {
            LOGGER.error("MCSReader error: " + e.getMessage(), e);
            cassandraTaskInfoDAO.dropTask(task.getTaskId(), "The task was dropped because " + e.getMessage(), TaskState.DROPPED.toString());
        } finally {
            //TODO spradzić czy zamykanie moze wyrzucic wyjatek oraz rozważyć autoclosable
            reader.close();
        }
    }




    private void executeForFilesList() {
        for (String file : task.getDataEntry(InputDataType.FILE_URLS)) {
            submitRecord(file);
        }
    }



    private void executeForDatasets() throws Exception {


        int expectedSize = 0;
        try {
            for (String dataSetUrl : task.getDataEntry(InputDataType.DATASET_URLS)) {
                expectedSize+= executeForOneDataSet(dataSetUrl);
            }
            cassandraTaskInfoDAO.setUpdateExpectedSize(task.getTaskId(), expectedSize);
        } finally {
             if (expectedSize == 0)
                cassandraTaskInfoDAO.dropTask(task.getTaskId(), "The task was dropped because it is empty", TaskState.DROPPED.toString());
        }


    }

    private int executeForOneDataSet(String dataSetUrl) throws MCSException, InterruptedException, ExecutionException {
        int expectedSize=0;
        try {

            final UrlParser urlParser = new UrlParser(dataSetUrl);
            if (urlParser.isUrlToDataset()) {
                if (getRevisionName() != null && getRevisionProvider() != null) {
                    expectedSize += executeForRevision(urlParser.getPart(UrlPart.DATA_SETS), urlParser.getPart(UrlPart.DATA_PROVIDERS));
                }else{
                    expectedSize += executeForEntireDataset(task, urlParser);

                }

            } else {
                LOGGER.warn("dataSet url is not formulated correctly {}", dataSetUrl);
                emitErrorNotification(dataSetUrl, "dataSet url is not formulated correctly", "");
            }

        } catch (MalformedURLException ex) {
            LOGGER.error("MCSReaderSpout error, Error while parsing DataSet URL : {}", ex.getMessage());
            emitErrorNotification(dataSetUrl, ex.getMessage(), task.getParameters().toString());
        }
        return expectedSize;
    }

    private int executeForEntireDataset(DpsTask dpsTask,UrlParser urlParser) {
        int expectedSize=0;
        RepresentationIterator iterator = reader.getRepresentationsOfEntireDataset(urlParser);
        while (iterator.hasNext() && !taskStatusChecker.hasKillFlag(dpsTask.getTaskId())) {
            expectedSize += submitRecordsForAllFilesOfRepresentation(iterator.next());
        }
        return expectedSize;
    }




    private int executeForRevision(String datasetName, String datasetProvider) throws MCSException, DriverException, InterruptedException, ConcurrentModificationException, ExecutionException {
        int maxRecordsCount = Optional.ofNullable(task.getParameter(PluginParameterKeys.SAMPLE_SIZE)).map(Integer::parseInt).orElse(Integer.MAX_VALUE);
        int count = 0;
        String startFrom = null;
        final long taskId = task.getTaskId();
        ExecutorService executorService = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
        Set<Future<Integer>> futures = new HashSet<>(INTERNAL_THREADS_NUMBER);
        int total = 0;
        do {

            ResultSlice<CloudIdAndTimestampResponse> slice = getCloudIdsChunk(datasetName, datasetProvider, startFrom);
            List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = slice.getResults();
            total += cloudIdAndTimestampResponseList.size();
            if (total > maxRecordsCount)
                if (total - maxRecordsCount < MAX_BATCH_SIZE)
                    cloudIdAndTimestampResponseList = cloudIdAndTimestampResponseList.subList(0, maxRecordsCount % MAX_BATCH_SIZE);
                else
                    break;

            final List<CloudIdAndTimestampResponse> finalCloudIdAndTimestampResponseList = cloudIdAndTimestampResponseList;
            futures.add(executorService.submit(() -> executeGettingFileUrlsForCloudIdList(finalCloudIdAndTimestampResponseList)));

            if (futures.size() >= INTERNAL_THREADS_NUMBER*MAX_BATCH_SIZE) {
                count += getCountAndWait(futures);
            }
            startFrom = slice.getNextSlice();
        }
        while (startFrom != null && !taskStatusChecker.hasKillFlag(taskId));

        if (futures.size() > 0)
            count += getCountAndWait(futures);
        executorService.shutdown();
        return count;
    }

    private ResultSlice<CloudIdAndTimestampResponse> getCloudIdsChunk(String datasetName, String datasetProvider, String startFrom) throws MCSException {
        if (getRevisionTimestamp() != null) {
            ResultSlice<CloudTagsResponse> chunk = reader.getDataSetRevisionsChunk(getRepresentationName(), getRevisionName(), getRevisionProvider(), getRevisionTimestamp(), datasetProvider, datasetName, startFrom);
           return toCloudAndTimestampResponse(chunk,getRevisionTimestamp());
        }else {
            return reader.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(getRepresentationName(), getRevisionName(), getRevisionProvider(), datasetName, datasetProvider, startFrom);
        }
    }

    private Integer executeGettingFileUrlsForCloudIdList(List<CloudIdAndTimestampResponse> responseList) throws MCSException {
        int count=0;
        for (CloudIdAndTimestampResponse response : responseList) {
            count+=executeGettingFileUrlsForOneCloudId(response);
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
        if (representation != null) {
            for (eu.europeana.cloud.common.model.File file : representation.getFiles()) {
                String fileUrl = "";
                if (!taskStatusChecker.hasKillFlag(task.getTaskId())) {
                    try {
                        fileUrl = reader.getFileUri(representation,file.getFileName());
                        submitRecord(fileUrl);
                        count++;
                    } catch (Exception e) {
                        LOGGER.warn("Error while getting File URI from MCS {}", e.getMessage());
                        count++;
                        emitErrorNotification(fileUrl, "Error while getting File URI from MCS " + e.getMessage(), "");
                    }
                } else
                    break;
            }
        } else {
            LOGGER.warn("Problem while reading representation");
        }
        return count;
    }

    private void startProgressing() {
        LOGGER.info("Start progressing for Task with id {}", task.getTaskId());
        cassandraTaskInfoDAO.updateTask(task.getTaskId(), "", String.valueOf(TaskState.CURRENTLY_PROCESSING), new Date());
    }

    private boolean taskContainsFileUrls() {
        return task.getInputData().get(FILE_URLS) != null;

    }

    private ResultSlice<CloudIdAndTimestampResponse> toCloudAndTimestampResponse(ResultSlice<CloudTagsResponse> chunk, String revisionTimestamp) {
        return new ResultSlice<>(chunk.getNextSlice(),
                chunk.getResults()
                     .stream()
                     .map(response -> new CloudIdAndTimestampResponse(response.getCloudId(), Date.from(Instant.parse(revisionTimestamp))))
                     .collect(Collectors.toList()));
    }

    private void emitErrorNotification(String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(task.getTaskId(),
                resource, RecordState.ERROR, message, additionalInformations);
        // insertError(task.getTaskId(),message,additionalInformations,)
    }

    private void insertError(long taskId, String errorMessage, String additionalInformation, String errorType, String resource) {
        Retriever.retryOnError3Times("Error while inserting Error to cassandra.", () ->
                taskErrorDAO.insertError(taskId, errorType, errorMessage, resource, additionalInformation));

    }
    private void submitRecord(String fileUrl) {
        DpsRecord record=DpsRecord.builder().taskId(task.getTaskId()).inputData(fileUrl).build();
        recordSubmitService.submitRecord(record, topicName);
    }

    private int getCountAndWait(Set<Future<Integer>> futures) throws InterruptedException, ExecutionException {
        int count = 0;
        for (Future<Integer> future : futures) {
            count += future.get();
        }
        futures.clear();
        return count;
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


}
