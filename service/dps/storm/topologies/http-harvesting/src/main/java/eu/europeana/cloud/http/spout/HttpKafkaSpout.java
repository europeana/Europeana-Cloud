package eu.europeana.cloud.http.spout;

import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.States;
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
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CustomKafkaSpout;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.utils.TaskSpoutInfo;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

/**
 * Created by Tarek on 4/27/2018.
 */
public class HttpKafkaSpout extends CustomKafkaSpout {

    private SpoutOutputCollector collector;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpKafkaSpout.class);

    private static final int BATCH_MAX_SIZE = 1240 * 4;
    private static final String CLOUD_SEPARATOR = "_";
    private static final String MAC_TEMP_FOLDER = "__MACOSX";
    private static final String MAC_TEMP_FILE = ".DS_Store";


    private transient ConcurrentHashMap<Long, TaskSpoutInfo> cache;

    HttpKafkaSpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }

    public HttpKafkaSpout(SpoutConfig spoutConf, String hosts, int port, String keyspaceName,
                          String userName, String password) {
        super(spoutConf, hosts, port, keyspaceName, userName, password);

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
                    StormTaskTuple stormTaskTuple = new StormTaskTuple(
                            dpsTask.getTaskId(),
                            dpsTask.getTaskName(),
                            dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS).get(0), null, dpsTask.getParameters(), dpsTask.getOutputRevision(), new OAIPMHHarvestingDetails());
                    execute(stormTaskTuple);
                    cache.remove(taskId);
                }
            }
        } catch (Exception e) {
            LOGGER.error("StaticDpsTaskSpout error: {}", e.getMessage());
            if (dpsTask != null)
                cassandraTaskInfoDAO.dropTask(dpsTask.getTaskId(), "The task was dropped because " + e.getMessage(), TaskState.DROPPED.toString());


        }
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

    private void emitErrorNotification(long taskId, String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.ERROR, message, additionalInformations);
        collector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    public void execute(StormTaskTuple stormTaskTuple) throws CompressionExtensionNotRecognizedException {
        File file = null;
        try {
            String httpURL = stormTaskTuple.getFileUrl();
            file = downloadFile(httpURL);
            String compressingExtension = FilenameUtils.getExtension(file.getName());
            FileUnpackingService fileUnpackingService = UnpackingServiceFactory.createUnpackingService(compressingExtension);
            fileUnpackingService.unpackFile(file.getAbsolutePath(), file.getParent() + File.separator);
            Path start = Paths.get(new File(file.getParent()).toURI());
            emitFiles(start, stormTaskTuple);
            cassandraTaskInfoDAO.setUpdateExpectedSize(stormTaskTuple.getTaskId(), cache.get(stormTaskTuple.getTaskId()).getFileCount());
        } catch (IOException e) {
            LOGGER.error("HTTPHarvesterBolt error: {} ", e.getMessage());
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), "Error while reading the files because of " + e.getMessage(), "");
        } finally {
            removeTempFolder(file);
        }
    }

    private File downloadFile(String httpURL) throws IOException {
        URL url = new URL(httpURL);
        URLConnection conn = url.openConnection();
        InputStream inputStream = conn.getInputStream();
        OutputStream outputStream = null;
        try {
            String tempFileName = UUID.randomUUID().toString();
            String folderPath = Files.createTempDirectory(tempFileName) + File.separator;
            File file = new File(folderPath + FilenameUtils.getName(httpURL));
            outputStream = new FileOutputStream(file.toPath().toString());
            byte[] buffer = new byte[BATCH_MAX_SIZE];
            IOUtils.copyLarge(inputStream, outputStream, buffer);
            return file;
        } finally {
            if (outputStream != null)
                outputStream.close();
            if (inputStream != null)
                inputStream.close();
        }
    }


    private void emitFiles(final Path start, final StormTaskTuple stormTaskTuple) throws IOException {
        final boolean useDefaultIdentifiers = useDefaultIdentifier(stormTaskTuple);
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
                        emitFileContent(stormTaskTuple, filePath, readableFileName, mimeType, useDefaultIdentifiers);
                    } catch (IOException | EuropeanaIdException e){
                        emitErrorNotification(stormTaskTuple.getTaskId(), readableFileName, "Error while reading the file because of " + e.getMessage(), "");
                    }
                    cache.get(stormTaskTuple.getTaskId()).inc();
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
    }


    private String getFileNameFromPath(Path path) {
        if (path != null)
            return path.getFileName().toString();
        throw new IllegalArgumentException("Path parameter should never be null");
    }

    private void emitFileContent(StormTaskTuple stormTaskTuple, String filePath, String readableFilePath, String mimeType, boolean useDefaultIdentifiers) throws IOException, EuropeanaIdException {
        FileInputStream fileInputStream = null;
        try {
            StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
            File file = new File(filePath);
            fileInputStream = new FileInputStream(file);
            tuple.setFileData(fileInputStream);
            tuple.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, mimeType);

            String localId;
            if(useDefaultIdentifiers)
                localId = formulateLocalId(readableFilePath);
            else {
                EuropeanaGeneratedIdsMap europeanaIdentifier = getEuropeanaIdentifier(tuple);
                localId = europeanaIdentifier.getEuropeanaGeneratedId();
                tuple.addParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, europeanaIdentifier.getSourceProvidedChoAbout());
            }
            tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, localId);
            tuple.setFileUrl(readableFilePath);
            collector.emit(tuple.toStormTuple());
        } finally {
            if (fileInputStream != null)
                fileInputStream.close();
        }
    }

    private boolean useDefaultIdentifier(StormTaskTuple stormTaskTuple) {
        boolean useDefaultIdentifiers = false;
        if ("true".equals(stormTaskTuple.getParameter(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS))) {
            useDefaultIdentifiers = true;
        }
        return useDefaultIdentifiers;
    }

    private EuropeanaGeneratedIdsMap getEuropeanaIdentifier(StormTaskTuple stormTaskTuple) throws EuropeanaIdException {
        String datasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        if(StringUtils.isEmpty(datasetId))
            throw new EuropeanaIdException("METIS dataset id not provided");

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