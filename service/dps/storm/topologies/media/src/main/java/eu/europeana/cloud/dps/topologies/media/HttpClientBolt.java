package eu.europeana.cloud.dps.topologies.media;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData.Status;
import eu.europeana.cloud.dps.topologies.media.support.TempFileSync;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.LinkCheckingException;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.temp.HttpClientCallback;
import eu.europeana.metis.mediaprocessing.temp.TemporaryMediaProcessor;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class HttpClientBolt<O> extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientBolt.class);

    private static final int FOLLOW_REDIRECTS = 3;

    protected transient OutputCollector outputCollector;

    private TemporaryMediaProcessor mediaProcessor;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        final MediaProcessorFactory mediaProcessorFactory = new MediaProcessorFactory();
        mediaProcessorFactory.setMaxRedirectCount(FOLLOW_REDIRECTS);
        try {
            mediaProcessor = (TemporaryMediaProcessor) mediaProcessorFactory.createMediaExtractor();
        } catch (MediaProcessorException e) {
            throw new RuntimeException("Could not initialize http client", e);
        }
    }

    @Override
    public final void cleanup() {
        mediaProcessor.close();
        TempFileSync.stopServer();
    }

    @Override
    public final void execute(final Tuple input) {

        final MediaTupleData data = (MediaTupleData) input.getValueByField(MediaTupleData.FIELD_NAME);
        final List<FileInfo> files = data.getFileInfos();
        final StatsTupleData stats = new StatsTupleData(data.getTaskId(), files.size());

        final HttpClientCallback<FileInfo, O> callback = (fileInfo, output, status) -> {
            synchronized (stats) {
                statusUpdate(input, stats, fileInfo, output, status == null ? Status.STATUS_OK : status);
            }
        };

        try {
            execute(mediaProcessor, files, data.getConnectionLimitsPerSource(), callback);
        } catch (MediaExtractionException | LinkCheckingException e) {
            outputCollector.fail(input);
        }
    }

    protected abstract void execute(MediaExtractor mediaProcessor, List<FileInfo> files,
        Map<String, Integer> connectionLimitsPerSource, HttpClientCallback<FileInfo, O> callback)
        throws MediaExtractionException, LinkCheckingException;

    protected abstract void statusUpdate(Tuple tuple, StatsTupleData stats, FileInfo fileInfo,
        O output, String status);
}
