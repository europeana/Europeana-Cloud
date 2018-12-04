package eu.europeana.cloud.dps.topologies.media;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData.Status;
import eu.europeana.cloud.dps.topologies.media.support.TempFileSync;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.model.Resource;
import eu.europeana.metis.mediaprocessing.model.ResourceImpl;
import eu.europeana.metis.mediaprocessing.temp.HttpClientCallback;
import eu.europeana.metis.mediaprocessing.temp.TemporaryMediaProcessor;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadBolt extends HttpClientBolt<Resource> {

    private static final Logger logger = LoggerFactory.getLogger(DownloadBolt.class);

    /**
     * Tuple stream dedicated for records with large downloaded resources. Goes to
     * bolts on the same machine to save network traffic.
     * @see #localStreamThreshold
     */
    public static final String STREAM_LOCAL = "download-local";

    private InetAddress localAddress;

    private long localStreamThreshold;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(MediaTupleData.FIELD_NAME, StatsTupleData.FIELD_NAME));
        declarer.declareStream(STREAM_LOCAL, new Fields(MediaTupleData.FIELD_NAME, StatsTupleData.FIELD_NAME));
        declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        localAddress = TempFileSync.startServer(stormConf);
        localStreamThreshold = (long) stormConf.getOrDefault("MEDIATOPOLOGY_FILE_TRANSFER_THRESHOLD_MB", 10) * 1024 * 1024;
    }

    @Override
    protected void execute(eu.europeana.metis.mediaprocessing.MediaProcessor mediaProcessor,
        List<FileInfo> files, Map<String, Integer> connectionLimitsPerSource,
        HttpClientCallback<FileInfo, Resource> callback) throws MediaProcessorException {
        ((TemporaryMediaProcessor) mediaProcessor).executeDownloadTask(files, connectionLimitsPerSource, callback);
    }

    @Override
    protected void statusUpdate(Tuple tuple, StatsTupleData stats, FileInfo fileInfo, Resource resource, String status) {

        // Copy values to the FileInfo instance.
        if (Status.STATUS_OK.equals(status)) {
            fileInfo.setMimeType(resource.getMimeType());
            if (resource.hasContent()) {
                fileInfo.setContent(((ResourceImpl)resource).getContentPath().toFile());
            }
        } else {
            fileInfo.setErrorFlag(true);
        }

        // Further process a download
        if (status.equals(Status.STATUS_OK) && fileInfo.getContent() != null
            && !fileInfo.isErrorFlagSet()) {

            fileInfo.setContentSource(localAddress);

            long now = System.currentTimeMillis();
            stats.setDownloadEndTime(now);
            if (stats.getDownloadStartTime() == 0) {
              // too hard to determine actual start time, not significant for global measure
              stats.setDownloadStartTime(now);
            }
            stats.setDownloadedBytes(stats.getDownloadedBytes() + fileInfo.getContent().length());
        }

        // Add the statistics: check if all resources are processed to finalize statistics.
        stats.addStatus(fileInfo.getUrl(), status);
        if (stats.getStatuses().size() == stats.getResourceCount()) {
            boolean someSuccess = stats.getErrors().size() < stats.getResourceCount();
            if (someSuccess) {
                MediaTupleData data = (MediaTupleData) tuple.getValueByField(MediaTupleData.FIELD_NAME);
                data.getFileInfos().removeIf(FileInfo::isErrorFlagSet);
                if (stats.getDownloadedBytes() < localStreamThreshold) {
                    outputCollector.emit(tuple, new Values(data, stats));
                } else {
                    outputCollector.emit(STREAM_LOCAL, tuple, new Values(data, stats));
                }
            } else {
                outputCollector.emit(StatsTupleData.STREAM_ID, tuple, new Values(stats));
            }
            outputCollector.ack(tuple);
        }
    }
}
