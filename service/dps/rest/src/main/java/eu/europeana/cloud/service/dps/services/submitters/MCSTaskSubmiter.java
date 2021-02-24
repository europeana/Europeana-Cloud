package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.RevisionIdentifier;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
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

    private final TaskStatusUpdater taskStatusUpdater;

    private final RecordSubmitService recordSubmitService;

    private final String mcsClientURL;

    public MCSTaskSubmiter(TaskStatusChecker taskStatusChecker, TaskStatusUpdater taskStatusUpdater, RecordSubmitService recordSubmitService, String mcsClientURL) {
        this.taskStatusChecker = taskStatusChecker;
        this.taskStatusUpdater = taskStatusUpdater;
        this.recordSubmitService = recordSubmitService;
        this.mcsClientURL = mcsClientURL;
    }

    public void execute(SubmitTaskParameters submitParameters) {
        DpsTask task = submitParameters.getTask();
        try {
            LOGGER.info("Sending task id={} to topology {} by kafka topic {}. Parameters:\n{}",
                    task.getTaskId(), submitParameters.getTopologyName(), submitParameters.getTopicName(), submitParameters);

            checkIfTaskIsKilled(task);

            logProgress(submitParameters, 0);
            int expectedSize;
            if (taskContainsFileUrls(task)) {
                expectedSize = executeForFilesList(submitParameters);
            } else {
                expectedSize = executeForDatasetList(submitParameters);
            }

            checkIfTaskIsKilled(task);
            if (expectedSize != 0) {
                taskStatusUpdater.updateStatusExpectedSize(task.getTaskId(), TaskState.QUEUED.toString(), expectedSize);
                LOGGER.info("Submitting {} records of task id={} to Kafka succeeded.", expectedSize, task.getTaskId());
            } else {
                taskStatusUpdater.setTaskDropped(task.getTaskId(), "The task was dropped because it is empty");
                LOGGER.warn("The task id={} was dropped because it is empty.", task.getTaskId());
            }

        } catch (SubmitingTaskWasKilled e) {
            LOGGER.warn(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error("MCSTaskSubmiter error for taskId={}", task.getTaskId(), e);
            taskStatusUpdater.setTaskDropped(task.getTaskId(), "The task was dropped because " + e.getMessage());
        }
    }

    private MCSReader createMcsReader(SubmitTaskParameters submitParameters) {
        String authorizationHeader = submitParameters.getTask().getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        return new MCSReader(mcsClientURL, authorizationHeader);
    }

    private int executeForFilesList(SubmitTaskParameters submitParameters) {
        List<String> filesList = submitParameters.getTask().getDataEntry(FILE_URLS);
        int count = 0;
        for (String file : filesList) {
            if (submitRecord(file, submitParameters, false)) {
                count++;
            }
        }
        return count;
    }

    private int executeForDatasetList(SubmitTaskParameters submitParameters) throws InterruptedException, ExecutionException, MCSException {
        int expectedSize = 0;
        for (String dataSetUrl : submitParameters.getTask().getDataEntry(InputDataType.DATASET_URLS)) {
            expectedSize += executeForOneDataSet(dataSetUrl, submitParameters);
        }
        return expectedSize;
    }

    private int executeForOneDataSet(String dataSetUrl, SubmitTaskParameters submitParameters) throws InterruptedException, ExecutionException, MCSException {
        try (MCSReader reader = createMcsReader(submitParameters)) {
            UrlParser urlParser = new UrlParser(dataSetUrl);
            if (!urlParser.isUrlToDataset()) {
                throw new TaskSubmitException("DataSet URL is not formulated correctly: " + dataSetUrl);
            }

            int expectedSize = 0;
            if (submitParameters.hasInputRevision()) {
                expectedSize += executeForRevision(urlParser.getPart(UrlPart.DATA_SETS), urlParser.getPart(UrlPart.DATA_PROVIDERS), submitParameters, reader);
            } else {
                expectedSize += executeForEntireDataset(urlParser, submitParameters, reader);

            }
            return expectedSize;

        } catch (MalformedURLException e) {
            throw new TaskSubmitException("MCSTaskSubmiter error, Error while parsing DataSet URL : \"" + dataSetUrl + "\"", e);
        }
    }

    private int executeForEntireDataset(UrlParser urlParser, SubmitTaskParameters submitParameters, MCSReader reader) {
        int expectedSize = 0;
        RepresentationIterator iterator = reader.getRepresentationsOfEntireDataset(urlParser);
        while (iterator.hasNext()) {
            checkIfTaskIsKilled(submitParameters.getTask());
            expectedSize += submitRecordsForRepresentation(iterator.next(), submitParameters, false);
        }
        return expectedSize;
    }

    private int executeForRevision(String datasetName, String datasetProvider, SubmitTaskParameters submitParameters, MCSReader reader) throws InterruptedException, ExecutionException, MCSException {
        ExecutorService executor = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
        try {
            DpsTask task = submitParameters.getTask();
            int maxRecordsCount = submitParameters.getMaxRecordsCount();
            int count = 0;
            String startFrom = null;
            Set<Future<Integer>> futures = new HashSet<>(INTERNAL_THREADS_NUMBER);
            int total = 0;
            do {
                checkIfTaskIsKilled(task);
                ResultSlice<CloudIdAndTimestampResponse> slice = getCloudIdsChunk(datasetName, datasetProvider, startFrom, submitParameters, reader);
                List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = slice.getResults();

                int maxRecordsLeft = maxRecordsCount - total;
                if (cloudIdAndTimestampResponseList.size() > maxRecordsLeft) {
                    cloudIdAndTimestampResponseList = cloudIdAndTimestampResponseList.subList(0, maxRecordsLeft);
                }
                total += cloudIdAndTimestampResponseList.size();

                final List<CloudIdAndTimestampResponse> finalCloudIdAndTimestampResponseList = cloudIdAndTimestampResponseList;
                futures.add(executor.submit(() -> executeGettingFileUrlsForCloudIdList(finalCloudIdAndTimestampResponseList, submitParameters, reader)));

                if (futures.size() >= INTERNAL_THREADS_NUMBER * MAX_BATCH_SIZE) {
                    count += getCountAndWait(futures);
                }
                startFrom = slice.getNextSlice();
            }
            while ((startFrom != null) && (total < maxRecordsCount));

            if (!futures.isEmpty())
                count += getCountAndWait(futures);

            return count;
        } finally {
            executor.shutdown();
        }
    }

    private ResultSlice<CloudIdAndTimestampResponse> getCloudIdsChunk(String datasetName, String datasetProvider, String startFrom, SubmitTaskParameters submitTaskParameters, MCSReader reader) throws MCSException {
        if (submitTaskParameters.getInputRevision().getCreationTimeStamp() != null) {
            ResultSlice<CloudTagsResponse> chunk = reader.getDataSetRevisionsChunk(
                    submitTaskParameters.getRepresentationName(),
                    submitTaskParameters.getInputRevision(),
                    datasetProvider, datasetName, startFrom);
            return toCloudAndTimestampResponse(chunk, submitTaskParameters.getInputRevision().getCreationTimeStamp());
        } else {
            return reader.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(
                    submitTaskParameters.getRepresentationName(),
                    submitTaskParameters.getInputRevision().getRevisionName(),
                    submitTaskParameters.getInputRevision().getRevisionProviderId(),
                    datasetName, datasetProvider, startFrom);
        }
    }

    private Integer executeGettingFileUrlsForCloudIdList(List<CloudIdAndTimestampResponse> responseList, SubmitTaskParameters submitParameters, MCSReader reader) throws MCSException {
        int count = 0;
        checkIfTaskIsKilled(submitParameters.getTask());
        for (CloudIdAndTimestampResponse response : responseList) {
            count += executeGettingFileUrlsForOneCloudId(response, submitParameters, reader);
        }

        return count;
    }

    private int executeGettingFileUrlsForOneCloudId(CloudIdAndTimestampResponse response, SubmitTaskParameters submitParameters, MCSReader reader) throws MCSException {

        int count = 0;
        List<Representation> representations = reader.getRepresentationsByRevision(
                submitParameters.getRepresentationName(),
                submitParameters.getInputRevision().getRevisionName(),
                submitParameters.getInputRevision().getRevisionProviderId(),
                response.getRevisionTimestamp(), response.getCloudId());
        for (Representation representation : representations) {
            RevisionIdentifier inputRevision = submitParameters.getInputRevision().withCreationTimeStamp(response.getRevisionTimestamp());
            count += submitRecordsForRepresentation(representation, submitParameters,
                    isMarkedAsDeleted(
                            representation,
                            inputRevision));
        }
        return count;
    }

    private int submitRecordsForRepresentation(Representation representation, SubmitTaskParameters submitParameters, boolean markedAsDeleted) {
        if (representation == null) {
            throw new TaskSubmitException("Problem while reading representation - representation is null.");
        }

        if(markedAsDeleted){
            return submitRecordForDeletedRepresentation(representation, submitParameters) ;
        }else{
            return submitRecordsForAllFilesOfRepresentation(representation, submitParameters) ;
        }
    }

    private int submitRecordForDeletedRepresentation(Representation representation, SubmitTaskParameters submitParameters) {
        if (submitRecord(representation.getUri().toString(), submitParameters, true)) {
            return 1;
        } else {
            return 0;
        }
    }

    private int submitRecordsForAllFilesOfRepresentation(Representation representation, SubmitTaskParameters submitParameters) {
        int count = 0;

        for (File file : representation.getFiles()) {
            checkIfTaskIsKilled(submitParameters.getTask());

            String fileUrl = file.getContentUri().toString();
            if (submitRecord(fileUrl, submitParameters, false)) {
                count++;
            }

        }

        return count;
    }

    private ResultSlice<CloudIdAndTimestampResponse> toCloudAndTimestampResponse(ResultSlice<CloudTagsResponse> chunk, Date revisionTimestamp) {
        return new ResultSlice<>(chunk.getNextSlice(),
                chunk.getResults()
                        .stream()
                        .map(response -> new CloudIdAndTimestampResponse(response.getCloudId(), revisionTimestamp))
                        .collect(Collectors.toList()));
    }

    private boolean submitRecord(String fileUrl, SubmitTaskParameters submitParameters, boolean markedAsDeleted) {
        DpsTask task = submitParameters.getTask();
        DpsRecord record = DpsRecord.builder().taskId(task.getTaskId()).metadataPrefix(submitParameters.getSchemaName())
                .recordId(fileUrl).markedAsDeleted(markedAsDeleted).build();

        boolean increaseCounter = recordSubmitService.submitRecord(record, submitParameters);
        logProgress(submitParameters, submitParameters.incrementAndGetPerformedRecordCounter());
        return increaseCounter;
    }

    private void logProgress(SubmitTaskParameters submitParameters, int submitedCount) {
        if (submitedCount % 1000 == 0) {
            LOGGER.info("Task id={} records submiting is progressing. Already submited: {} records",
                    submitParameters.getTask().getTaskId(), submitedCount);
        }
    }

    private boolean taskContainsFileUrls(DpsTask task) {
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

    private boolean isMarkedAsDeleted(Representation representation, RevisionIdentifier revision) {
        return findRevision(representation, revision).isDeleted();
    }

    private Revision findRevision(Representation representation, RevisionIdentifier revisionToBeFound) {

        for (Revision revision : representation.getRevisions()) {
            if(revisionToBeFound.identifies(revision)){
                return revision;
            }
        }
        throw new TaskSubmitException("Revision of name " + revisionToBeFound.getRevisionName() + " and timestamp " + revisionToBeFound.getCreationTimeStamp() + " not found for representation " + representation);
    }

    private void checkIfTaskIsKilled(DpsTask task) {
        if (taskStatusChecker.hasKillFlag(task.getTaskId())) {
            throw new SubmitingTaskWasKilled(task);
        }
    }
}
