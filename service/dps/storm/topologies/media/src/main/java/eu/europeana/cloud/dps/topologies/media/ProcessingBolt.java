package eu.europeana.cloud.dps.topologies.media;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.TempFileSync;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.metis.mediaprocessing.RdfSerializerImpl;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.exception.RdfConverterException;
import eu.europeana.metis.mediaprocessing.exception.RdfSerializationException;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.mediaprocessing.temp.TemporaryMediaService;
import eu.europeana.metis.mediaprocessing.temp.TemporaryMediaService.MediaProcessingListener;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import javax.ws.rs.ProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessingBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(ProcessingBolt.class);

    private static AmazonS3 amazonClient;
    private static String storageBucket = "";

    private transient OutputCollector outputCollector;
    private transient Map<String, Object> config;

    private transient TemporaryMediaService mediaService;
    private transient RdfSerializerImpl serializer;

    private ArrayList<ResultsUploadThread> threads = new ArrayList<>();
    private ArrayBlockingQueue<Item> queue = new ArrayBlockingQueue<>(100);

    private boolean persistResult;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.config = stormConf;
        try {
            mediaService = new TemporaryMediaService();
            serializer = new RdfSerializerImpl();
        } catch (RdfConverterException | MediaProcessorException e) {

        }

        persistResult = (Boolean) config.getOrDefault("MEDIATOPOLOGY_RESULT_PERSIST", true);

        int threadCount = (int) (long) config.getOrDefault("MEDIATOPOLOGY_RESULT_UPLOAD_THREADS", 2);
        for (int i = 0; i < threadCount; i++) {
            ResultsUploadThread thread = new ResultsUploadThread(i);
            thread.start();
            threads.add(thread);
        }

        TempFileSync.init(stormConf);

        synchronized (ProcessingBolt.class) {
            if (amazonClient == null) {
                amazonClient = new AmazonS3Client(new BasicAWSCredentials(
                        (String) config.get("AWS_CREDENTIALS_ACCESSKEY"),
                        (String) config.get("AWS_CREDENTIALS_SECRETKEY")));
                amazonClient.setEndpoint((String) config.get("AWS_CREDENTIALS_ENDPOINT"));
                storageBucket = (String) config.get("AWS_CREDENTIALS_BUCKET");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
    }

    private static class MediaProcessingListenerImpl implements MediaProcessingListener<FileInfo> {
      
        private final StatsTupleData statsData;
        private boolean retry = false;
        private long start;
  
        public MediaProcessingListenerImpl(StatsTupleData statsData) {
            this.statsData = statsData;
        }

        @Override
        public void beforeStartingFile(FileInfo file) throws MediaExtractionException {
            start = System.currentTimeMillis();
            TempFileSync.ensureLocal(file);
        }
  
        @Override
        public void handleMediaExtractionException(FileInfo file, MediaExtractionException e) {
            logger.info("processing failed ({}) for {}", e.getMessage(), file.getUrl());
            logger.trace("failure details:", e);
            String message = e.getMessage();
            if (message == null) {
                logger.warn("Exception without proper report error:", e);
                message = "UNKNOWN";
            }
            statsData.addStatus(file.getUrl(), message);
        }
  
        @Override
        public void handleOtherException(FileInfo file, Exception e) {
            logger.info("processing failed ({}) for {}", e.getMessage(), file.getUrl());
            statsData.addStatus(file.getUrl(), e.getMessage());
        }
  
        @Override
        public void afterCompletingFile(FileInfo file) {
            logger.debug("Processing {} took {} ms", file.getUrl(), System.currentTimeMillis() - start);
        }
    }
    
    
    @Override
    public void execute(Tuple input) {
        MediaTupleData mediaData = (MediaTupleData) input.getValueByField(MediaTupleData.FIELD_NAME);
        StatsTupleData statsData = (StatsTupleData) input.getValueByField(StatsTupleData.FIELD_NAME);

        statsData.setProcessingStartTime(System.currentTimeMillis());
        RDF rdf = mediaData.getEdm();
        
        final MediaProcessingListenerImpl listener = new MediaProcessingListenerImpl(statsData);
        final Pair<RDF, List<Thumbnail>> mediaProcessResult = mediaService.performMediaProcessing(rdf,
            mediaData.getFileInfos(), listener);
        
        if (listener.retry) {
            cleanupRecord(mediaData, mediaProcessResult.getRight());
            outputCollector.fail(input);
        } else {
            queueUpload(input, mediaData, statsData, mediaProcessResult.getLeft(), mediaProcessResult.getRight());
        }
        statsData.setProcessingEndTime(System.currentTimeMillis());
    }

    @Override
    public void cleanup() {
        mediaService.close();
        for (ResultsUploadThread thread : threads)
            thread.interrupt();
    }

    private void cleanupRecord(MediaTupleData mediaData, List<Thumbnail> thumbnails) {
        for (FileInfo fileInfo : mediaData.getFileInfos()) {
            TempFileSync.delete(fileInfo);
        }
        for (Thumbnail thumb : thumbnails) {
            try {
                thumb.close();
            } catch (IOException e) {
                logger.warn("Could not delete thumbnail.", e);
            }
        }
    }

    private void queueUpload(Tuple tuple, MediaTupleData mediaData, StatsTupleData statsData, RDF rdf, List<Thumbnail> thumbnails) {
        Item item = new Item();
        item.tuple = tuple;
        item.mediaData = mediaData;
        item.statsData = statsData;
        item.thumbnails = thumbnails;
        try {
            item.edmContents = serializer.serialize(rdf);
        } catch (RdfSerializationException e) {
            throw new IllegalStateException("Should always be able to serialize.", e);
        }
        try {
            if (queue.remainingCapacity() == 0)
                logger.warn("Results upload queue full");
            queue.put(item);
        } catch (InterruptedException e) {
            logger.trace("Thread interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    private static class Item {
        Tuple tuple;
        StatsTupleData statsData;
        MediaTupleData mediaData;
        List<Thumbnail> thumbnails;
        byte[] edmContents;
    }

    private class ResultsUploadThread extends Thread {

        FileServiceClient fileClient;
        RecordServiceClient recordClient;
        RevisionServiceClient revisionServiceClient;
        DataSetServiceClient dataSetServiceClient;

        Item currentItem;

        ResultsUploadThread(int id) {
            super("result-uploader-" + id);

        }

        @Override
        public void run() {
            try {
                while (true) {
                    StatsTupleData statsData;
                    DpsTask previousTask = currentItem == null ? null : currentItem.mediaData.getTask();
                    currentItem = queue.take();
                    try {
                        DpsTask currentTask = currentItem.mediaData.getTask();
                        if (!currentTask.equals(previousTask)) {
                            fileClient = Util.getFileServiceClient(config, currentTask);
                            recordClient = Util.getRecordServiceClient(config, currentTask);
                            revisionServiceClient = Util.getRevisionServiceClient(config, currentTask);
                            dataSetServiceClient = Util.getDataSetServiceClient(config, currentTask);
                        }

                        statsData = currentItem.statsData;
                        statsData.setUploadStartTime(System.currentTimeMillis());
                        saveThumbnails();
                        saveMetadata();
                        statsData.setUploadEndTime(System.currentTimeMillis());
                    } finally {
                        cleanupRecord(currentItem.mediaData, currentItem.thumbnails);
                    }

                    outputCollector.emit(StatsTupleData.STREAM_ID, currentItem.tuple, new Values(statsData));
                    outputCollector.ack(currentItem.tuple);
                }
            } catch (InterruptedException e) {
                logger.trace("result upload thread finishing", e);
                Thread.currentThread().interrupt();
                while ((currentItem = queue.poll()) != null) {
                    cleanupRecord(currentItem.mediaData, currentItem.thumbnails);
                }
            } catch (Throwable t) {
                logger.error("result upload thread failure", t);
            }
        }

        void saveMetadata() {
            long start = System.currentTimeMillis();
            DpsTask task = currentItem.mediaData.getTask();

            String representationName = getParamOrDefault(task, PluginParameterKeys.NEW_REPRESENTATION_NAME);
            String fileName = getParamOrDefault(task, PluginParameterKeys.OUTPUT_FILE_NAME);
            String mediaType = getParamOrDefault(task, PluginParameterKeys.OUTPUT_MIME_TYPE);

            try (ByteArrayInputStream bais = new ByteArrayInputStream(currentItem.edmContents)) {
                if (currentItem.edmContents.length == 0)
                    throw new IOException("Result EDM contents missing");

                Representation r = currentItem.mediaData.getEdmRepresentation();
                if (persistResult) {
                    URI rep = recordClient.createRepresentation(r.getCloudId(), representationName, r.getDataProvider(),
                            bais, fileName, mediaType);
                    addRevisionToSpecificResource(rep);
                    addRepresentationToDataSets(rep);
                    logger.debug("saved tech metadata in {} ms: {}", System.currentTimeMillis() - start, rep);
                } else {
                    URI rep =
                            recordClient.createRepresentation(r.getCloudId(), representationName, r.getDataProvider());
                    addRevisionToSpecificResource(rep);
                    addRepresentationToDataSets(rep);
                    String version = new UrlParser(rep.toString()).getPart(UrlPart.VERSIONS);
                    if (StringUtils.isBlank(fileName))
                        fileName = UUID.randomUUID().toString();
                    fileClient.uploadFile(r.getCloudId(), representationName, version, fileName, bais,
                            mediaType);
                    recordClient.deleteRepresentation(r.getCloudId(), representationName, version);
                    logger.debug("tech metadata saving simulation took {} ms", System.currentTimeMillis() - start);
                }
            } catch (IOException | DriverException | MCSException | ProcessingException e) {
                logger.error("Could not store tech metadata representation in "
                        + currentItem.mediaData.getEdmRepresentation().getCloudId(), e);
                for (FileInfo file : currentItem.mediaData.getFileInfos()) {
                    currentItem.statsData.addErrorIfAbsent(file.getUrl(), "TECH METADATA SAVING");
                }
            }
        }

        void saveThumbnails() {
            long start = System.currentTimeMillis();
            boolean uploaded = false;
            for (Thumbnail t : currentItem.thumbnails) {
                try {
                    amazonClient.putObject(storageBucket, t.getTargetName(), t.getContentStream(), new ObjectMetadata());
                    logger.debug("thumbnail saved: {} b, md5({}) = {}", t.getContentSize(), t.getResourceUrl(), t.getTargetName());
                    uploaded = true;
                } catch (AmazonClientException | IOException e) {
                    logger.error("Could not save thumbnails for " + t.getResourceUrl(), e);
                    currentItem.statsData.addErrorIfAbsent(t.getResourceUrl(), "THUMBNAIL SAVING");
                }
            }
            if (!uploaded) {
                logger.info("No thumbnails were uploaded for {}",
                        currentItem.mediaData.getEdmRepresentation().getCloudId());
            }
            logger.debug("thumbnail saving took {} ms ", System.currentTimeMillis() - start);
        }

        void addRevisionToSpecificResource(URI affectedResourceURL) throws MalformedURLException, MCSException {
            Revision outputRevision = currentItem.mediaData.getTask().getOutputRevision();
            if (outputRevision != null) {
                final UrlParser urlParser = new UrlParser(affectedResourceURL.toString());
                if (outputRevision.getCreationTimeStamp() == null)
                    outputRevision.setCreationTimeStamp(new Date());
                revisionServiceClient.addRevision(
                        urlParser.getPart(UrlPart.RECORDS),
                        urlParser.getPart(UrlPart.REPRESENTATIONS),
                        urlParser.getPart(UrlPart.VERSIONS),
                        outputRevision);
            } else {
                logger.info("Revisions list is empty");
            }
        }

        void addRepresentationToDataSets(URI rep) throws MalformedURLException, MCSException {
            String datasets = getParamOrDefault(currentItem.mediaData.getTask(), PluginParameterKeys.OUTPUT_DATA_SETS);
            if (StringUtils.isBlank(datasets)) {
                logger.info("Output data sets list is empty");
                return;
            }
            UrlParser fileUrl = new UrlParser(rep.toString());
            if (!fileUrl.isUrlToRepresentationVersionFile()) {
                logger.error("invalid representation file url: {}", rep);
                return;
            }
            for (String datasetLocation : datasets.split(",")) {
                UrlParser datasetUrl = new UrlParser(datasetLocation);
                if (!datasetUrl.isUrlToDataset()) {
                    logger.error("invalid target dataset url: {}", datasetLocation);
                    continue;
                }
                dataSetServiceClient.assignRepresentationToDataSet(
                        datasetUrl.getPart(UrlPart.DATA_PROVIDERS),
                        datasetUrl.getPart(UrlPart.DATA_SETS),
                        fileUrl.getPart(UrlPart.RECORDS),
                        fileUrl.getPart(UrlPart.REPRESENTATIONS),
                        fileUrl.getPart(UrlPart.VERSIONS));
            }
        }

        String getParamOrDefault(DpsTask task, String paramKey) {
            String param = task.getParameter(paramKey);
            return param != null ? param : PluginParameterKeys.PLUGIN_PARAMETERS.get(paramKey);
        }
    }
}
