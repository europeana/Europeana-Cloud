package eu.europeana.cloud.http.spout;

import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.http.common.CompressionFileExtension;
import eu.europeana.cloud.http.common.UnpackingServiceFactory;
import eu.europeana.cloud.http.exceptions.CompressionExtensionNotRecognizedException;
import eu.europeana.cloud.http.service.FileUnpackingService;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CollectorWrapper;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CustomKafkaSpout;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.TaskQueueFiller;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

/**
 * Created by Tarek on 4/27/2018.
 */
public class HttpKafkaSpout extends CustomKafkaSpout {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpKafkaSpout.class);

    private static final int BATCH_MAX_SIZE = 1240 * 4;
    private static final String CLOUD_SEPARATOR = "_";
    private static final String MAC_TEMP_FOLDER = "__MACOSX";
    private static final String MAC_TEMP_FILE = ".DS_Store";

    private SpoutOutputCollector collector;

    TaskDownloader taskDownloader;

    HttpKafkaSpout(SpoutConfig spoutConf) {
        super(spoutConf);
        taskDownloader = new TaskDownloader();
    }

    public HttpKafkaSpout(SpoutConfig spoutConf, String hosts, int port, String keyspaceName,
                          String userName, String password) {
        super(spoutConf, hosts, port, keyspaceName, userName, password);

    }


    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        taskDownloader = new TaskDownloader();
        this.collector = collector;
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
            LOGGER.error("Spout error: {}", e.getMessage());
            if (stormTaskTuple != null)
                cassandraTaskInfoDAO.setTaskDropped(stormTaskTuple.getTaskId(), "The task was dropped because " + e.getMessage());
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
            cassandraTaskInfoDAO.setTaskDropped(dpsTask.getTaskId(), "The task was dropped because of redeployment");
    }

    private void deactivateCurrentTask() {
        DpsTask currentDpsTask = taskDownloader.getCurrentDpsTask();
        if (currentDpsTask != null) {
            cassandraTaskInfoDAO.setTaskDropped(currentDpsTask.getTaskId(), "The task was dropped because of redeployment");
        }
    }


    final class TaskDownloader extends Thread implements TaskQueueFiller {
        private static final int MAX_SIZE = 100;
        public static final String ERROR_WHILE_READING_A_FILE_MESSAGE = "Error while reading a file";
        ArrayBlockingQueue<DpsTask> taskQueue = new ArrayBlockingQueue<>(MAX_SIZE);
        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(MAX_SIZE);
        private DpsTask currentDpsTask;


        public TaskDownloader() {
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
                        startProgress(currentDpsTask.getTaskId());
                        stormTaskTuple = new StormTaskTuple(
                                currentDpsTask.getTaskId(),
                                currentDpsTask.getTaskName(),
                                currentDpsTask.getDataEntry(InputDataType.REPOSITORY_URLS).get(0), null, currentDpsTask.getParameters(), currentDpsTask.getOutputRevision(), new OAIPMHHarvestingDetails());
                        execute(stormTaskTuple);
                    } else {
                        LOGGER.info("Skipping DROPPED task {}", currentDpsTask.getTaskId());
                    }
                } catch (Exception e) {
                    LOGGER.error("StaticDpsTaskSpout error: {}", e.getMessage());
                    if (stormTaskTuple != null)
                        cassandraTaskInfoDAO.setTaskDropped(stormTaskTuple.getTaskId(), "The task was dropped because " + e.getMessage());
                }
            }
        }

        private void startProgress(long taskId) {
            LOGGER.info("Start progressing for Task with id {}", currentDpsTask.getTaskId());
            cassandraTaskInfoDAO.updateTask(taskId, "", String.valueOf(TaskState.CURRENTLY_PROCESSING), new Date());

        }

        private DpsTask getCurrentDpsTask() {
            return currentDpsTask;
        }


        private void emitErrorNotification(long taskId, String resource, String message, String additionalInformations) {
            NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                    resource, RecordState.ERROR, message, additionalInformations);
            collector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
        }

        void execute(StormTaskTuple stormTaskTuple) throws CompressionExtensionNotRecognizedException, IOException, InterruptedException {
            File file = null;
            int expectedSize = 0;
            try {
                final boolean useDefaultIdentifiers = useDefaultIdentifier(stormTaskTuple);
                String metisDatasetId = null;
                if (!useDefaultIdentifiers) {
                    metisDatasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
                    if (StringUtils.isEmpty(metisDatasetId)) {
                        cassandraTaskInfoDAO.setTaskDropped(stormTaskTuple.getTaskId(), "The task was dropped because METIS_DATASET_ID not provided");
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
                    cassandraTaskInfoDAO.setTaskDropped(stormTaskTuple.getTaskId(), "The task was dropped because it is empty");

            }
        }

        private File downloadFile(String httpURL) throws IOException {
            URL url = new URL(httpURL);
            URLConnection conn = url.openConnection();

            String tempFileName = UUID.randomUUID().toString();
            String folderPath = Files.createTempDirectory(tempFileName) + File.separator;
            File file = new File(folderPath + FilenameUtils.getName(httpURL));
            byte[] buffer = new byte[BATCH_MAX_SIZE];

            try (InputStream inputStream = conn.getInputStream();
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
                        } catch (IOException | EuropeanaIdException e) {
                            LOGGER.error(e.getMessage());
                            emitErrorNotification(stormTaskTuple.getTaskId(), readableFileName, ERROR_WHILE_READING_A_FILE_MESSAGE, ERROR_WHILE_READING_A_FILE_MESSAGE + ": " + file.getFileName() + " because of " + e.getCause());
                        } catch (InterruptedException e) {
                            LOGGER.error(e.getMessage());
                            emitErrorNotification(stormTaskTuple.getTaskId(), readableFileName, ERROR_WHILE_READING_A_FILE_MESSAGE, ERROR_WHILE_READING_A_FILE_MESSAGE + ": " + file.getFileName() + " because of " + e.getCause());
                            Thread.currentThread().interrupt();
                        }
                        finally {
                            expectedSize.set(expectedSize.incrementAndGet());
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


        private String getFileNameFromPath(Path path) {
            if (path != null)
                return path.getFileName().toString();
            throw new IllegalArgumentException("Path parameter should never be null");
        }

        private void prepareTuple(StormTaskTuple stormTaskTuple, String filePath, String readableFilePath, String mimeType, boolean useDefaultIdentifiers, String datasetId) throws IOException, InterruptedException, EuropeanaIdException {
            try (FileInputStream fileInputStream = new FileInputStream(new File(filePath));) {
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
}