package eu.europeana.cloud.dps.topologies.media;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.metis.mediaprocessing.MediaProcessor;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.temp.HttpClientCallback;
import eu.europeana.metis.mediaprocessing.temp.TemporaryMediaProcessor;
import java.util.List;
import java.util.Map;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinkCheckBolt extends HttpClientBolt<Void> {

    private static final Logger logger = LoggerFactory.getLogger(LinkCheckBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
    }

    @Override
    protected void execute(MediaProcessor mediaProcessor, List<FileInfo> files,
        Map<String, Integer> connectionLimitsPerSource, HttpClientCallback<FileInfo, Void> callback)
        throws MediaProcessorException {
        ((TemporaryMediaProcessor) mediaProcessor).executeLinkCheckTask(files, connectionLimitsPerSource, callback);
    }

    @Override
    protected void statusUpdate(Tuple tuple, StatsTupleData stats, FileInfo fileInfo, Void output, String status) {
        stats.addStatus(fileInfo.getUrl(), status);
        if (stats.getStatuses().size() == stats.getResourceCount()) {
            outputCollector.emit(StatsTupleData.STREAM_ID, tuple, new Values(stats));
            outputCollector.ack(tuple);
        }
    }
}
