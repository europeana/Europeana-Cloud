package eu.europeana.cloud.service.dps.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Last bolt in topology. 
 * This bolt send success notification to notification bolt.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class EndBolt extends BaseBasicBolt
{
    public static final String NOTIFICATION_STREAM_NAME = AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) 
    {
        StormTaskTuple t = StormTaskTuple.fromStormTuple(tuple);
        emitSuccessNotification(boc, t.getTaskId(), t.getFileUrl(), "", t.getParameters().toString());
    }
    
    private void emitSuccessNotification(BasicOutputCollector collector, 
            long taskId, String resource, String message, String additionalInformations)
    {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId, 
                resource, NotificationTuple.States.SUCCESS, message, additionalInformations);
        collector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }
}
