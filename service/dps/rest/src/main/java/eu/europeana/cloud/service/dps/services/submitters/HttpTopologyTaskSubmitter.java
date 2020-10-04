package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.http.*;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class HttpTopologyTaskSubmitter implements TaskSubmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTopologyTaskSubmitter.class);

    private static final String MAC_TEMP_FILE = ".DS_Store";
    private static final String MAC_TEMP_FOLDER = "__MACOSX";
    private final TaskStatusUpdater taskStatusUpdater;
    private final FilesCounterFactory filesCounterFactory;
    private final KafkaTopicSelector kafkaTopicSelector;
    private final TaskStatusChecker taskStatusChecker;
    private final RecordExecutionSubmitService recordSubmitService;
    private final FileURLCreator fileURLCreator;

    @Value("${harvestingTasksDir}")
    private String harvestingTasksDir;

    public HttpTopologyTaskSubmitter(TaskStatusUpdater taskStatusUpdater,
                                     FilesCounterFactory filesCounterFactory,
                                     RecordExecutionSubmitService recordSubmitService,
                                     KafkaTopicSelector kafkaTopicSelector,
                                     TaskStatusChecker taskStatusChecker,
                                     FileURLCreator fileURLCreator) {
        this.taskStatusUpdater = taskStatusUpdater;
        this.filesCounterFactory = filesCounterFactory;
        this.recordSubmitService = recordSubmitService;
        this.kafkaTopicSelector = kafkaTopicSelector;
        this.taskStatusChecker = taskStatusChecker;
        this.fileURLCreator = fileURLCreator;
    }

    @Override
    public void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {

        LOGGER.info("HTTP task submission for {} started.", parameters.getTask().getTaskId());

        if (parameters.isRestarted()) {
            LOGGER.info("The task {} in Http Topology cannot be restarted.", parameters.getTask().getTaskId());
            taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task in Http Topology cannot be restarted. It will be dropped.");
            return;
        }

        int expectedCount = getFilesCountInsideTask(parameters.getTask(), parameters.getTopologyName());
        LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);

        try {
            File downloadedFile = downloadFileFor(parameters.getTask());
            File extractedFile = extractFile(downloadedFile);
            //
            expectedCount = iterateOverFiles(extractedFile, parameters.getTask());
            taskStatusUpdater.setUpdateExpectedSize(parameters.getTask().getTaskId(), expectedCount);

        } catch (IOException | CompressionExtensionNotRecognizedException e) {
            taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task was dropped because of errors: " + e.getMessage());

        }
        parameters.setExpectedSize(expectedCount);

        updateTaskStatus(parameters);

        LOGGER.info("HTTP task submission for {} finished. {} records submitted.",
                parameters.getTask().getTaskId(),
                expectedCount);

    }

    private int getFilesCountInsideTask(DpsTask task, String topologyName) throws TaskSubmissionException {
        FilesCounter filesCounter = filesCounterFactory.createFilesCounter(task, topologyName);
        return filesCounter.getFilesCount(task);
    }

    private File downloadFileFor(DpsTask dpsTask) throws IOException {
        String urlToZipFile = dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS).get(0);
        return new FileDownloader().downloadFile(urlToZipFile, downloadedFileLocationFor(dpsTask));
    }

    private String downloadedFileLocationFor(DpsTask dpsTask) {
        return harvestingTasksDir + File.separator + "task_" + dpsTask.getTaskId();
    }

    private File extractFile(File downloadedFile) throws CompressionExtensionNotRecognizedException, IOException {
        String compressingExtension = FilenameUtils.getExtension(downloadedFile.getName());
        FileUnpackingService fileUnpackingService;
        fileUnpackingService = UnpackingServiceFactory.createUnpackingService(compressingExtension);
        fileUnpackingService.unpackFile(downloadedFile.getAbsolutePath(), downloadedFile.getParent() + File.separator);
        return new File(downloadedFile.getParent());
    }

    private int iterateOverFiles(File extractedFile, DpsTask dpsTask) throws IOException {
        final AtomicInteger expectedSize = new AtomicInteger(0);
        Files.walkFileTree(Paths.get(extractedFile.toURI()), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (taskStatusChecker.hasKillFlag(dpsTask.getTaskId()))
                    return FileVisitResult.TERMINATE;
                String fileName = getFileNameFromPath(file);
                if (fileName.equals(MAC_TEMP_FILE))
                    return FileVisitResult.CONTINUE;
                String extension = FilenameUtils.getExtension(file.toString());
                if (!CompressionFileExtension.contains(extension)) {
                    DpsRecord dpsRecord = DpsRecord.builder()
                            .taskId(dpsTask.getTaskId())
                            .recordId(fileURLCreator.generateUrlFor(dpsTask, fileName))
                            .build();
                    recordSubmitService.submitRecord(
                            dpsRecord,
                            kafkaTopicSelector.findPreferredTopicNameFor("http_topology"));
                    expectedSize.addAndGet(1);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                String dirName = getFileNameFromPath(dir);
                if (dirName.equals(MAC_TEMP_FOLDER))
                    return FileVisitResult.SKIP_SUBTREE;
                return FileVisitResult.CONTINUE;
            }
        });
        return expectedSize.get();
    }

    private String getFileNameFromPath(Path path) {
        if (path != null)
            return path.getFileName().toString();
        throw new IllegalArgumentException("Path parameter should never be null");
    }

    private void updateTaskStatus(SubmitTaskParameters submitTaskParameters) {
        if (submitTaskParameters.getExpectedSize() == 0) {
            taskStatusUpdater.setTaskDropped(submitTaskParameters.getTask().getTaskId(), "The task doesn't include any records");
        } else {
            taskStatusUpdater.updateStatusExpectedSize(submitTaskParameters.getTask().getTaskId(), TaskState.SENT.toString(), submitTaskParameters.getExpectedSize());
        }

    }
}
