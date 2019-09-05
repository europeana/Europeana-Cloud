package eu.europeana.cloud.http.spout.job;

import com.google.common.base.Throwables;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.http.common.CompressionFileExtension;
import eu.europeana.cloud.http.common.UnpackingServiceFactory;
import eu.europeana.cloud.http.exceptions.CompressionExtensionNotRecognizedException;
import eu.europeana.cloud.http.service.FileUnpackingService;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

public class TaskExecutor implements Callable<Void> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

    public static final String ERROR_WHILE_READING_A_FILE_MESSAGE = "Error while reading a file";
    private static final int BATCH_MAX_SIZE = 1240 * 4;
    private static final String MAC_TEMP_FOLDER = "__MACOSX";
    private static final String MAC_TEMP_FILE = ".DS_Store";
    private static final String CLOUD_SEPARATOR = "_";

    private SpoutOutputCollector collector;
    private TaskStatusChecker taskStatusChecker;
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;

    private ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls;
    private StormTaskTuple stormTaskTuple;
    private DpsTask dpsTask;

    public TaskExecutor(SpoutOutputCollector collector, TaskStatusChecker taskStatusChecker,
                        CassandraTaskInfoDAO cassandraTaskInfoDAO, ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls,
                        StormTaskTuple stormTaskTuple, DpsTask dpsTask){
        this.collector = collector;
        this.taskStatusChecker = taskStatusChecker;
        this.cassandraTaskInfoDAO = cassandraTaskInfoDAO;

        this.tuplesWithFileUrls = tuplesWithFileUrls;
        this.stormTaskTuple = stormTaskTuple;
        this.dpsTask = dpsTask;
    }

    @Override
    public Void call() {
        try {
            execute();
        } catch (Exception e) {
            if(dpsTask != null) {
                cassandraTaskInfoDAO.dropTask(dpsTask.getTaskId(),
                        "The task was dropped because of " + e.getMessage() + ". The full exception is" +
                                Throwables.getStackTraceAsString(e), TaskState.DROPPED.toString());
            }
        }
        return null;
    }

    private void emitErrorNotification(long taskId, String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.ERROR, message, additionalInformations);
        collector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    void execute() throws CompressionExtensionNotRecognizedException, IOException, InterruptedException {
        File file = null;
        int expectedSize = 0;
        try {
            final boolean useDefaultIdentifiers = useDefaultIdentifier(stormTaskTuple);
            String metisDatasetId = null;
            if (!useDefaultIdentifiers) {
                metisDatasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
                if (StringUtils.isEmpty(metisDatasetId)) {
                    cassandraTaskInfoDAO.dropTask(stormTaskTuple.getTaskId(), "The task was dropped because METIS_DATASET_ID not provided", TaskState.DROPPED.toString());
                    return;
                }
            }
            String httpURL = stormTaskTuple.getFileUrl();
            file = downloadFile(httpURL);
            String compressingExtension = FilenameUtils.getExtension(file.getName());
            FileUnpackingService fileUnpackingService = UnpackingServiceFactory.createUnpackingService(compressingExtension);
            fileUnpackingService.unpackFile(file.getAbsolutePath(), file.getParent() + File.separator);
            Path start = Paths.get(new File(file.getParent()).toURI());
            expectedSize = iterateOverFiles(start, stormTaskTuple, useDefaultIdentifiers, metisDatasetId);
            cassandraTaskInfoDAO.setUpdateExpectedSize(stormTaskTuple.getTaskId(), expectedSize);
        } finally {
            removeTempFolder(file);
            if (expectedSize == 0)
                cassandraTaskInfoDAO.dropTask(stormTaskTuple.getTaskId(), "The task was dropped because it is empty", TaskState.DROPPED.toString());

        }
    }

    private String getFileNameFromPath(Path path) {
        if (path != null)
            return path.getFileName().toString();
        throw new IllegalArgumentException("Path parameter should never be null");
    }

    private void prepareTuple(StormTaskTuple stormTaskTuple, String filePath, String readableFilePath, String mimeType, boolean useDefaultIdentifiers, String datasetId) throws IOException, InterruptedException, EuropeanaIdException {
        try(FileInputStream fileInputStream = new FileInputStream(new File(filePath));) {
            StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
            tuple.setFileData(fileInputStream);
            tuple.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, mimeType);

            String localId;
            if (useDefaultIdentifiers)
                localId = formulateLocalId(readableFilePath);
            else {
                EuropeanaGeneratedIdsMap europeanaIdentifier = getEuropeanaIdentifier(tuple, datasetId);
                localId = europeanaIdentifier.getEuropeanaGeneratedId();
                tuple.addParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, europeanaIdentifier.getSourceProvidedChoAbout());
            }
            tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, localId);
            tuple.setFileUrl(readableFilePath);
            tuplesWithFileUrls.put(tuple);
        }
    }

    private boolean useDefaultIdentifier(StormTaskTuple stormTaskTuple) {
        boolean useDefaultIdentifiers = false;
        if ("true".equals(stormTaskTuple.getParameter(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS))) {
            useDefaultIdentifiers = true;
        }
        return useDefaultIdentifiers;
    }

    private File downloadFile(String httpURL) throws IOException {
        URL url = new URL(httpURL);
        URLConnection conn = url.openConnection();

        String tempFileName = UUID.randomUUID().toString();
        String folderPath = Files.createTempDirectory(tempFileName) + File.separator;
        File file = new File(folderPath + FilenameUtils.getName(httpURL));
        byte[] buffer = new byte[BATCH_MAX_SIZE];

        try(InputStream inputStream = conn.getInputStream();
            OutputStream outputStream = new FileOutputStream(file.toPath().toString())) {
            IOUtils.copyLarge(inputStream, outputStream, buffer);
            return file;
        }
    }

    private int iterateOverFiles(final Path start, final StormTaskTuple stormTaskTuple, final boolean useDefaultIdentifiers, final String metisDatasetId) throws IOException, InterruptedException {
        final AtomicInteger expectedSize = new AtomicInteger(0);
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                if (taskStatusChecker.hasKillFlag(stormTaskTuple.getTaskId()))
                    return FileVisitResult.TERMINATE;
                String fileName = getFileNameFromPath(file);
                if (fileName.equals(MAC_TEMP_FILE))
                    return FileVisitResult.CONTINUE;
                String extension = FilenameUtils.getExtension(file.toString());
                if (!CompressionFileExtension.contains(extension)) {
                    String mimeType = Files.probeContentType(file);
                    String filePath = file.toString();
                    String readableFileName = filePath.substring(start.toString().length() + 1).replaceAll("\\\\", "/");
                    try {
                        prepareTuple(stormTaskTuple, filePath, readableFileName, mimeType, useDefaultIdentifiers, metisDatasetId);
                        expectedSize.set(expectedSize.incrementAndGet());
                    } catch (IOException | EuropeanaIdException e) {
                        LOGGER.error(e.getMessage());
                        emitErrorNotification(stormTaskTuple.getTaskId(), readableFileName, ERROR_WHILE_READING_A_FILE_MESSAGE, ERROR_WHILE_READING_A_FILE_MESSAGE + ": " + file.getFileName() + " because of " + e.getCause());
                    } catch (InterruptedException e) {
                        LOGGER.error(e.getMessage());
                        emitErrorNotification(stormTaskTuple.getTaskId(), readableFileName, ERROR_WHILE_READING_A_FILE_MESSAGE, ERROR_WHILE_READING_A_FILE_MESSAGE + ": " + file.getFileName() + " because of " + e.getCause());
                        Thread.currentThread().interrupt();
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

    private EuropeanaGeneratedIdsMap getEuropeanaIdentifier(StormTaskTuple stormTaskTuple, String datasetId) throws EuropeanaIdException {
        String document = new String(stormTaskTuple.getFileData());
        EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
        return europeanIdCreator.constructEuropeanaId(document, datasetId);
    }

    private String formulateLocalId(String readableFilePath) {
        return new StringBuilder(readableFilePath).append(CLOUD_SEPARATOR).append(UUID.randomUUID().toString()).toString();
    }

    private void removeTempFolder(File file) {
        if (file != null)
            try {
                FileUtils.deleteDirectory(new File(file.getParent()));
            } catch (IOException e) {
                LOGGER.error("ERROR while removing the temp Folder: {}", e.getMessage());
            }
    }


}
