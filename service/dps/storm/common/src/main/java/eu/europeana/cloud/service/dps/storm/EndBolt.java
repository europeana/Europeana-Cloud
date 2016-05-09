package eu.europeana.cloud.service.dps.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Last bolt in topology.
 * This bolt send success notification to notification bolt.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class EndBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(EndBolt.class);
    public static final String NOTIFICATION_STREAM_NAME = AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

    protected Map stormConfig;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }

    @Override
    public void execute(Tuple tuple) {
        StormTaskTuple t = StormTaskTuple.fromStormTuple(tuple);
        try {
            String resultResource = t.getParameter(PluginParameterKeys.OUTPUT_URL);
            Validate.notNull(resultResource);
            emitSuccessNotification(t.getTaskId(), t.getFileUrl(), "", "", resultResource);
        } catch (Exception ex) {
            LOGGER.error("Problem with send end notification because: {}",
                    ex);
            emitErrorNotification(t.getTaskId(), t.getFileUrl(), "", "");
        } finally {
            outputCollector.ack(tuple);
        }
        return;
    }

    private void emitSuccessNotification(long taskId, String resource,
                                         String message, String additionalInformations, String resultResource) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.SUCCESS, message, additionalInformations, resultResource);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    private void emitErrorNotification(long taskId, String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.ERROR, message, additionalInformations);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    @Override
    public void prepare(Map stormConfig, TopologyContext tc, OutputCollector oc) {
        this.stormConfig = stormConfig;
        this.topologyContext = tc;
        this.outputCollector = oc;
    }
}
