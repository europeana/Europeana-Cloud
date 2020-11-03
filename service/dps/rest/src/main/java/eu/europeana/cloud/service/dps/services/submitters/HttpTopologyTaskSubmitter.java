package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.http.*;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
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
import java.nio.file.attribute.PosixFilePermission;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Service
public class HttpTopologyTaskSubmitter implements TaskSubmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTopologyTaskSubmitter.class);

    private static final String MAC_TEMP_FILE = ".DS_Store";
    private static final String MAC_TEMP_FOLDER = "__MACOSX";
    private final TaskStatusUpdater taskStatusUpdater;
    private final FilesCounterFactory filesCounterFactory;
    private final KafkaTopicSelector kafkaTopicSelector;
    private final TaskStatusChecker taskStatusChecker;
    private final RecordSubmitService recordSubmitService;
    private final FileURLCreator fileURLCreator;

    @Value("${harvestingTasksDir}")
    private String harvestingTasksDir;

    public HttpTopologyTaskSubmitter(TaskStatusUpdater taskStatusUpdater,
                                     FilesCounterFactory filesCounterFactory,
                                     RecordSubmitService recordSubmitService,
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
            File extractedDirectory = extractFile(downloadedFile);
            //
            selectKafkaTopicFor(parameters);
            correctDirectoryRights(extractedDirectory.toPath());
            expectedCount = iterateOverFiles(extractedDirectory, parameters);
            updateTaskStatus(parameters.getTask(), expectedCount);

        } catch (IOException | CompressionExtensionNotRecognizedException e) {
            LOGGER.error("Unable to submit the task.", e);
            taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task was dropped because of errors: " + e.getMessage());
        }

        LOGGER.info("HTTP task submission for {} finished. {} records submitted.",
                parameters.getTask().getTaskId(),
                expectedCount);
    }

    /**
     * Method corrects rights on Linux systems, where created new directory and extracted files have not
     * right copied from parent folder, and they have not any right for others users. Also group is not
     * preserved from parent. It is a problem cause apache server could not reach files
     * cause it typically works as special apache_user.
     * The purpose of this method is to copy rights from parent directory, that should have correctly
     * configured right to passed as parameter directory and any directory or file inside.
     *
     * @param directory
     * @throws IOException
     */
    private void correctDirectoryRights(Path directory) throws IOException {
        try (Stream<Path> files = Files.walk(directory)) {
            Set<PosixFilePermission> rigths = Files.getPosixFilePermissions(directory.getParent());
            Iterator<Path> i = files.iterator();
            while (i.hasNext()) {
                Files.setPosixFilePermissions(i.next(), rigths);
            }
        } catch (UnsupportedOperationException e) {
            LOGGER.info("Not Posfix system. Need not correct rights");
        }
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
        FileUnpackingService fileUnpackingService = UnpackingServiceFactory.createUnpackingService(compressingExtension);
        fileUnpackingService.unpackFile(downloadedFile.getAbsolutePath(), downloadedFile.getParent() + File.separator);
        return new File(downloadedFile.getParent());
    }

    private void selectKafkaTopicFor(SubmitTaskParameters parameters) {
        parameters.setTopicName(kafkaTopicSelector.findPreferredTopicNameFor(TopologiesNames.HTTP_TOPOLOGY));
    }

    private int iterateOverFiles(File extractedDirectory, SubmitTaskParameters submitTaskParameters) throws IOException {
        final AtomicInteger expectedSize = new AtomicInteger(0);
        Files.walkFileTree(Paths.get(extractedDirectory.toURI()), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (taskStatusChecker.hasKillFlag(submitTaskParameters.getTask().getTaskId()))
                    return FileVisitResult.TERMINATE;
                String fileName = getFileNameFromPath(file);
                if(!fileEligibleForEmission(file))
                    return FileVisitResult.CONTINUE;
                String extension = FilenameUtils.getExtension(file.toString());
                if (!CompressionFileExtension.contains(extension)) {
                    DpsRecord dpsRecord = DpsRecord.builder()
                            .taskId(submitTaskParameters.getTask().getTaskId())
                            .recordId(fileURLCreator.generateUrlFor(submitTaskParameters.getTask(), fileName))
                            .build();
                    if (recordSubmitService.submitRecord(
                            dpsRecord,
                            submitTaskParameters)) {
                        expectedSize.addAndGet(1);
                    }
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

    private boolean fileEligibleForEmission(Path file) {
        String fileName = getFileNameFromPath(file);
        return !fileName.equals(MAC_TEMP_FILE);
    }

    private String getFileNameFromPath(Path path) {
        if (path != null)
            return path.getFileName().toString();
        throw new IllegalArgumentException("Path parameter should never be null");
    }

    private void updateTaskStatus(DpsTask dpsTask, int expectedCount) {
        if (!taskStatusChecker.hasKillFlag(dpsTask.getTaskId())) {
            if (expectedCount == 0) {
                taskStatusUpdater.setTaskDropped(dpsTask.getTaskId(), "The task doesn't include any records");
            } else {
                taskStatusUpdater.updateStatusExpectedSize(dpsTask.getTaskId(), TaskState.QUEUED.toString(), expectedCount);
            }
        }
    }
}
