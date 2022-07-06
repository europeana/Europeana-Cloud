package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import org.apache.storm.tuple.Tuple;

/**
 * Revision writer bolt used for Media topology.
 * It emits tuple to {@link eu.europeana.cloud.service.dps.storm.NotificationBolt} with or without error info.
 */
public class RevisionWriterBoltForMediaTopology extends RevisionWriterBolt {

    public RevisionWriterBoltForMediaTopology(String ecloudMcsAddress) {
        super(ecloudMcsAddress);
    }

    @Override
    protected void emitTuple(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        if (tupleContainsErrors(stormTaskTuple)) {
            emitSuccessNotificationContainingErrorInfo(anchorTuple, stormTaskTuple);
        } else {
            emitSuccessNotification(anchorTuple, stormTaskTuple);
        }
    }

    private boolean tupleContainsErrors(StormTaskTuple tuple) {
        return tuple.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null;
    }

    private void emitSuccessNotificationContainingErrorInfo(Tuple anchorTuple, StormTaskTuple tuple) {
        String resultUrl = tuple.getParameter(PluginParameterKeys.OUTPUT_URL);
        emitSuccessNotification(anchorTuple, tuple.getTaskId(), tuple.isMarkedAsDeleted(),
                tuple.getFileUrl(), "", "", resultUrl,
                tuple.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE),
                tuple.getParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE),
                StormTaskTupleHelper.getRecordProcessingStartTime(tuple));
    }

    private void emitSuccessNotification(Tuple anchorTuple, StormTaskTuple tuple) {
        String resultUrl = tuple.getParameter(PluginParameterKeys.OUTPUT_URL);
        emitSuccessNotification(anchorTuple, tuple.getTaskId(), tuple.isMarkedAsDeleted(),
                tuple.getFileUrl(), "", "", resultUrl,
                StormTaskTupleHelper.getRecordProcessingStartTime(tuple));
    }
}
