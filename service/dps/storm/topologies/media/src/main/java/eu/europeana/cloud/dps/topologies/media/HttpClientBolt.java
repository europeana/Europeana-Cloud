package eu.europeana.cloud.dps.topologies.media;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData.Status;
import eu.europeana.cloud.dps.topologies.media.support.TempFileSync;
import eu.europeana.metis.mediaprocessing.MediaProcessor;
import eu.europeana.metis.mediaprocessing.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.temp.FileInfo;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class HttpClientBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientBolt.class);

    private static final int FOLLOW_REDIRECTS = 3;

    protected transient OutputCollector outputCollector;

    private MediaProcessor mediaProcessor;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        final MediaProcessorFactory mediaProcessorFactory = new MediaProcessorFactory();
        mediaProcessorFactory.setRedirectCount(FOLLOW_REDIRECTS);
        mediaProcessorFactory.setGeneralConnectionLimit((int) (long) stormConf.getOrDefault("MEDIATOPOLOGY_CONNECTIONS_TOTAL", 200));
        mediaProcessorFactory.setConnectionLimitPerSource((int) (long) stormConf.getOrDefault("MEDIATOPOLOGY_CONNECTIONS_PER_SOURCE", 4));
        try {
            mediaProcessor = mediaProcessorFactory.createMediaProcessor();
        } catch (MediaProcessorException e) {
            throw new RuntimeException("Could not initialize http client", e);
        }
    }

    @Override
    public final void cleanup() {
        try {
            mediaProcessor.close();
        } catch (IOException e) {
            logger.error("HttpClient could not close", e);
        }
        TempFileSync.stopServer();
    }

    @Override
    public final void execute(final Tuple input) {

        final MediaTupleData data = (MediaTupleData) input.getValueByField(MediaTupleData.FIELD_NAME);
        final List<FileInfo> files = data.getFileInfos();
        final StatsTupleData stats = new StatsTupleData(data.getTaskId(), files.size());

        final BiConsumer<FileInfo, String> callback = (fileInfo, status) -> {
            synchronized (stats) {
                statusUpdate(input, stats, fileInfo, status == null ? Status.STATUS_OK : status);
            }
        };

        try {
            execute(mediaProcessor, files, data.getConnectionLimitsPerSource(), callback);
        } catch (MediaProcessorException e) {
            outputCollector.fail(input);
        }
    }

    protected abstract void execute(MediaProcessor mediaProcessor, List<FileInfo> files,
        Map<String, Integer> connectionLimitsPerSource, BiConsumer<FileInfo, String> callback)
        throws MediaProcessorException;

    protected abstract void statusUpdate(Tuple tuple, StatsTupleData stats, FileInfo fileInfo,
        String status);
}
