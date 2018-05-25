package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsInitTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.metis.mediaservice.EdmObject;
import eu.europeana.metis.mediaservice.MediaException;
import eu.europeana.metis.mediaservice.UrlType;

/**
 * Wraps a base spout that generates tuples with JSON encoded {@link DpsTask}s
 * (usually a {@link KafkaSpout}) and translates its output to handle one EDM
 * representation per tuple.
 */
public class DataSetReaderSpout extends BaseRichSpout {

    public static final String SOURCE_FIELD = "source";

    private static final Logger logger = LoggerFactory.getLogger(DataSetReaderSpout.class);

    private final IRichSpout baseSpout;
    private final Collection<UrlType> urlTypes;
    private transient SpoutOutputCollector outputCollector;
    private transient Map<String, Object> config;

    private transient ConcurrentHashMap<String, EdmInfo> edmsByStormMsgId;

    private transient ConcurrentHashMap<String, SourceInfo> sourcesByHost;

    private transient DatasetDownloader datasetDownloader;
    private transient EdmDownloader edmDownloader;

    private long emitLimit;

    public DataSetReaderSpout(IRichSpout baseSpout, Collection<UrlType> urlTypes) {
        this.baseSpout = baseSpout;
        this.urlTypes = urlTypes;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(MediaTupleData.FIELD_NAME, SOURCE_FIELD));
        declarer.declareStream(StatsInitTupleData.STREAM_ID, new Fields(StatsInitTupleData.FIELD_NAME));
        declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        outputCollector = collector;
        config = conf;

        edmsByStormMsgId = new ConcurrentHashMap<>();
        sourcesByHost = new ConcurrentHashMap<>();
        datasetDownloader = new DatasetDownloader();
        edmDownloader = new EdmDownloader();

        emitLimit = (Long) conf.getOrDefault("MEDIATOPOLOGY_DATASET_EMIT_LIMIT", Long.MAX_VALUE);

        baseSpout.open(conf, context, new CollectorWrapper(collector));
    }

    @Override
    public void close() {
        baseSpout.close();
        edmDownloader.stop();
    }

    @Override
    public void nextTuple() {
        baseSpout.nextTuple();
        Optional<SourceInfo> leastBusyActiveSource = sourcesByHost.values().stream()
                .filter(s -> !s.isEmpty())
                .min(Comparator.comparing(s -> s.running.size()));
        if (leastBusyActiveSource.isPresent()) {
            SourceInfo source = leastBusyActiveSource.get();
            EdmInfo edmInfo = source.removeFromQueue();
            TaskInfo taskInfo = edmInfo.taskInfo;
            long taskId = taskInfo.task.getTaskId();

            MediaTupleData tupleData = new MediaTupleData(taskId, edmInfo.representation);
            tupleData.setEdm(edmInfo.edmObject);
            tupleData.setFileInfos(edmInfo.fileInfos);
            tupleData.setConnectionLimitsPerSource(taskInfo.connectionLimitPerSource);
            tupleData.setTask(taskInfo.task);

            outputCollector.emit(new Values(tupleData, source.host), toStormMsgId(edmInfo));

            source.running.add(edmInfo);
        } else {
            // nothing to do
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        EdmInfo edmInfo = edmsByStormMsgId.get(msgId);
        if (edmInfo == null) {
            logger.warn("Unrecognied ACK: {}", msgId);
            return;
        }

        edmProcessed(edmInfo);
    }

    @Override
    public void fail(Object msgId) {
        EdmInfo edmInfo = edmsByStormMsgId.get(msgId);
        if (edmInfo == null) {
            logger.warn("Unrecognied FAIL: {}", msgId);
            return;
        }

        if (edmInfo.attempts > 0) {
            logger.info("FAIL received for {}, will retry ({} attempts left)", msgId, edmInfo.attempts);
            edmInfo.attempts--;
            edmInfo.sourceInfo.addToQueue(edmInfo);
        } else {
            logger.info("FAIL received for {}, no more retries", msgId);
            edmProcessed(edmInfo);
        }
    }

    private String toStormMsgId(EdmInfo edmInfo) {
        return Integer.toString(System.identityHashCode(edmInfo));
    }

    private void edmProcessed(EdmInfo edmInfo) {
        SourceInfo source = edmInfo.sourceInfo;
        source.running.remove(edmInfo);
        if (source.running.isEmpty() && source.isEmpty()) {
            logger.info("Finished all EDMs from source {}", source.host);
            sourcesByHost.remove(source.host);
        }
        edmFinished(edmInfo);
    }

    private void edmFinished(EdmInfo edmInfo) {
        TaskInfo task = edmInfo.taskInfo;
        int completed = task.edmsComplete.incrementAndGet();
        if (completed == task.edmsInDataset) {
            logger.info("Task {} finished", task.task.getTaskName());
            baseSpout.ack(task.baseMsgId);
        }
    }

    private static class TaskInfo {
        DpsTask task;
        Map<String, Integer> connectionLimitPerSource;
        Object baseMsgId;
        List<UrlParser> datasetUrls = new ArrayList<>();
        volatile int edmsInDataset = -1;
        AtomicInteger edmsComplete = new AtomicInteger(0);
        long startTime;
    }

    private static class SourceInfo {
        final String host;
        private ArrayDeque<EdmInfo> queue = new ArrayDeque<>();
        ConcurrentHashSet<EdmInfo> running = new ConcurrentHashSet<>();

        public SourceInfo(String host) {
            this.host = host;
        }

        public synchronized void addToQueue(EdmInfo edmInfo) {
            edmInfo.sourceInfo = this;
            queue.add(edmInfo);
        }

        public synchronized EdmInfo removeFromQueue() {
            return queue.remove();
        }

        public synchronized boolean isEmpty() {
            return queue.isEmpty();
        }
    }

    private static class EdmInfo {
        Representation representation;
        EdmObject edmObject;
        List<FileInfo> fileInfos;
        TaskInfo taskInfo;
        SourceInfo sourceInfo;
        int attempts = 5;
    }

    private final class EdmDownloader {

        ArrayBlockingQueue<EdmInfo> edmQueue = new ArrayBlockingQueue<>(1024 * 128);
        ArrayList<EdmDownloadThread> threads;

        public EdmDownloader() {
            int threadsCount = (int) config.getOrDefault("MEDIATOPOLOGY_REPRESENTATION_DOWNLOAD_THREADS", 1);
            threads = new ArrayList<>();
            for (int i = 0; i < threadsCount; i++) {
                EdmDownloadThread thread = new EdmDownloadThread(i);
                thread.start();
                threads.add(thread);
            }
        }

        public void queue(EdmInfo edmInfo) throws InterruptedException {
            edmQueue.put(edmInfo);
        }

        public void stop() {
            for (EdmDownloadThread thread : threads)
                thread.interrupt();
        }

        private class EdmDownloadThread extends Thread {

            DpsTask currentTask;
            FileServiceClient fileClient;
            EdmObject.Parser parser;

            public EdmDownloadThread(int id) {
                super("edm-downloader-" + id);
                parser = new EdmObject.Parser();
            }

            @Override
            public void run() {
                try {
                    while (true) {
                        EdmInfo edmInfo = edmQueue.take();
                        if (!edmInfo.taskInfo.task.equals(currentTask)) {
                            currentTask = edmInfo.taskInfo.task;
                            fileClient = Util.getFileServiceClient(config, currentTask);
                        }
                        downloadEdm(edmInfo);
                        if (edmInfo.edmObject != null)
                            queueEdmInfo(edmInfo);
                    }
                } catch (InterruptedException e) {
                    logger.trace("edm download thread finishing", e);
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    logger.error("edm download thread failure", t);
                }
            }

            void downloadEdm(EdmInfo edmInfo) throws InterruptedException {
                Representation rep = edmInfo.representation;
                try {
                    File file = rep.getFiles().get(0);
                    String fileUri = fileClient.getFileUri(rep.getCloudId(), rep.getRepresentationName(),
                            rep.getVersion(), file.getFileName()).toString();
                    try (InputStream is = fileClient.getFile(fileUri)) {
                        edmInfo.edmObject = parser.parseXml(is);
                    } catch (MediaException e) {
                        logger.info("EDM loading failed ({}/{}) for {}", e.reportError, e.getMessage(), rep.getFiles());
                        logger.trace("full exception:", e);
                        StatsTupleData stats = new StatsTupleData(edmInfo.taskInfo.task.getTaskId(), 1);
                        stats.addStatus(fileUri, e.reportError);
                        outputCollector.emit(StatsTupleData.STREAM_ID, new Values(stats));

                        edmFinished(edmInfo);
                    }
                } catch (Exception e) {
                    logger.error("Downloading edm failed for " + rep, e);
                    edmQueue.put(edmInfo); // retry later
                    Thread.sleep(1000);
                }
            }

            void queueEdmInfo(EdmInfo edmInfo) {
                Set<String> urls = edmInfo.edmObject.getResourceUrls(urlTypes).keySet();
                if (urls.isEmpty()) {
                    logger.error("content url missing in edm file representation: {}", edmInfo.representation);
                    return;
                }
                edmInfo.fileInfos = urls.stream().map(FileInfo::new).collect(Collectors.toList());

                Map<String, Long> hostCounts = urls.stream()
                        .collect(Collectors.groupingBy(url -> URI.create(url).getHost(), Collectors.counting()));
                Comparator<Entry<String, Long>> c = Comparator.comparing(Entry::getValue);
                c = c.thenComparing(Entry::getKey); // compiler weirdness, can't do it in one call
                String host = hostCounts.entrySet().stream().max(c).get().getKey();
                SourceInfo source = sourcesByHost.computeIfAbsent(host, SourceInfo::new);
                source.addToQueue(edmInfo);
            }
        }
    }

    private final class DatasetDownloader extends Thread {

        ArrayBlockingQueue<TaskInfo> taskQueue = new ArrayBlockingQueue<>(128);
        DataSetServiceClient datasetClient;
        RecordServiceClient recordClient;

        public DatasetDownloader() {
            super("dataset-downloader");
            start();
        }

        public void queue(TaskInfo taskInfo) {
            try {
                taskQueue.put(taskInfo);
            } catch (InterruptedException e) {
                logger.trace("Thread interrupted", e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    TaskInfo taskInfo = taskQueue.take();
                    try {
                        datasetClient = Util.getDataSetServiceClient(config, taskInfo.task);
                        recordClient = Util.getRecordServiceClient(config, taskInfo.task);
                        downloadDataset(taskInfo);
                    } catch (DriverException | MCSException e) {
                        logger.warn("Problem downloading datasets from task " + taskInfo.task.getTaskName(), e);
                        taskQueue.put(taskInfo); // retry later
                        Thread.sleep(1000);
                    }
                }
            } catch (InterruptedException e) {
                logger.trace("dataset download thread finishing", e);
                Thread.currentThread().interrupt();
            } catch (Exception t) {
                logger.error("dataset downlaod thread failure", t);
            }
        }

        void downloadDataset(TaskInfo taskInfo)
                throws InterruptedException, MCSException {
            int edmCount = 0;
            for (UrlParser datasetUrl : taskInfo.datasetUrls) {
                DatasetDownloaderConfig dConfig = new DatasetDownloaderConfig(taskInfo, datasetUrl);

                if (dConfig.revisionName != null && dConfig.revisionProvider != null) {
                    if (dConfig.revisionTimestamp != null) {
                        edmCount += prepareExactRevisionsDownload(dConfig);
                    } else {
                        edmCount += prepareLatestRevisionsDownload(dConfig);
                    }
                } else {
                    edmCount += prepareAllRepresentationsDownload(dConfig);
                }
            }
            datasetDownloaded(taskInfo, edmCount);
        }

        private int prepareExactRevisionsDownload(DatasetDownloaderConfig config)
                throws MCSException, InterruptedException {
            int edmCount = 0;
            TaskInfo taskInfo = config.taskInfo;
            List<CloudTagsResponse> cloudTagsResponses =
                    datasetClient.getDataSetRevisions(config.providerId,
                            config.datasetId,
                            config.representationName, config.revisionName, config.revisionProvider,
                            config.revisionTimestamp);
            for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
                if (edmCount >= emitLimit)
                    break;

                String responseCloudId = cloudTagsResponse.getCloudId();
                RepresentationRevisionResponse representationRevisionResponse =
                        recordClient.getRepresentationRevision(responseCloudId, config.representationName,
                                config.revisionName, config.revisionProvider, config.revisionTimestamp);
                Representation rep = recordClient.getRepresentation(responseCloudId, config.representationName,
                        representationRevisionResponse.getVersion());

                prepareEdmInfo(rep, taskInfo);
                edmCount++;
            }
            return edmCount;
        }

        private int prepareLatestRevisionsDownload(DatasetDownloaderConfig config)
                throws MCSException, InterruptedException {
            int edmCount = 0;
            TaskInfo taskInfo = config.taskInfo;

            List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = datasetClient
                    .getLatestDataSetCloudIdByRepresentationAndRevision(config.datasetId,
                            config.providerId, config.revisionProvider,
                            config.revisionName,
                            config.representationName, false);
            for (CloudIdAndTimestampResponse cloudIdAndTimestampResponse : cloudIdAndTimestampResponseList) {
                if (edmCount >= emitLimit)
                    break;

                String responseCloudId = cloudIdAndTimestampResponse.getCloudId();
                RepresentationRevisionResponse representationRevisionResponse =
                        recordClient.getRepresentationRevision(responseCloudId, config.representationName,
                                config.revisionName, config.revisionProvider, DateHelper.getUTCDateString(
                                        cloudIdAndTimestampResponse.getRevisionTimestamp()));
                Representation rep = recordClient.getRepresentation(responseCloudId, config.representationName,
                        representationRevisionResponse.getVersion());

                prepareEdmInfo(rep, taskInfo);
                edmCount++;
            }
            return edmCount;
        }

        private int prepareAllRepresentationsDownload(DatasetDownloaderConfig config) throws InterruptedException {
            int edmCount = 0;
            TaskInfo taskInfo = config.taskInfo;
            RepresentationIterator iterator = datasetClient.getRepresentationIterator(
                    config.providerId, config.datasetId);
            while (iterator.hasNext() && edmCount < emitLimit) {
                Representation rep = iterator.next();
                prepareEdmInfo(rep, taskInfo);
                edmCount++;
            }
            return edmCount;
        }

        private void prepareEdmInfo(Representation rep, TaskInfo taskInfo) throws InterruptedException {
            EdmInfo edmInfo = new EdmInfo();
            edmInfo.representation = rep;
            edmInfo.taskInfo = taskInfo;
            edmsByStormMsgId.put(toStormMsgId(edmInfo), edmInfo);
            edmDownloader.queue(edmInfo);
        }

        private void datasetDownloaded(TaskInfo taskInfo, int edmCount) {
            logger.debug("Downloaded {} representations from datasets {}", edmCount, taskInfo.datasetUrls.stream()
                    .map(du -> du.getPart(UrlPart.DATA_SETS)).collect(Collectors.toList()));
            outputCollector.emit(StatsInitTupleData.STREAM_ID, new Values(
                    new StatsInitTupleData(taskInfo.task.getTaskId(), taskInfo.startTime, edmCount)));
            taskInfo.edmsInDataset = edmCount;
        }

        private class DatasetDownloaderConfig {
            public final TaskInfo taskInfo;
            public final String datasetId;
            public final String providerId;

            public final String representationName;
            public final String revisionProvider;
            public final String revisionName;
            public final String revisionTimestamp;

            public DatasetDownloaderConfig(TaskInfo taskInfo, UrlParser datasetUrl) {
                DpsTask task = taskInfo.task;
                this.taskInfo = taskInfo;

                this.datasetId = datasetUrl.getPart(UrlPart.DATA_SETS);
                this.providerId = datasetUrl.getPart(UrlPart.DATA_PROVIDERS);

                representationName = task.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
                revisionName = task.getParameter(PluginParameterKeys.REVISION_NAME);
                revisionProvider = task.getParameter(PluginParameterKeys.REVISION_PROVIDER);
                revisionTimestamp = task.getParameter(PluginParameterKeys.REVISION_TIMESTAMP);
            }

        }

    }

    private class CollectorWrapper extends SpoutOutputCollector {

        public CollectorWrapper(ISpoutOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            try {
                DpsTask task = new ObjectMapper().readValue((String) tuple.get(0), DpsTask.class);
                TaskInfo taskInfo = new TaskInfo();
                taskInfo.task = task;
                taskInfo.baseMsgId = messageId;
                taskInfo.startTime = System.currentTimeMillis();
                for (String datasetUrl : task.getDataEntry(InputDataType.DATASET_URLS))
                    taskInfo.datasetUrls.add(new UrlParser(datasetUrl));

                taskInfo.connectionLimitPerSource = task.getParameters().entrySet().stream()
                        .filter(e -> e.getKey().startsWith("host.limit."))
                        .collect(Collectors.toMap(e -> e.getKey().replace("host.limit.", ""),
                                e -> Integer.valueOf(e.getValue())));

                datasetDownloader.queue(taskInfo);
                logger.info("Task {} parsed", task.getTaskName());
            } catch (IOException e) {
                logger.error("Task rejected ({}: {})\n{}", e.getClass().getSimpleName(), e.getMessage(), tuple.get(0));
                logger.debug("Exception details", e);
            }

            return Collections.emptyList();
        }
    }
}
