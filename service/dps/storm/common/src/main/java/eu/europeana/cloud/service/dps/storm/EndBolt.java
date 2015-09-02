package eu.europeana.cloud.service.dps.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;

/**
 * Last bolt in topology. 
 * This bolt send success notification to notification bolt.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class EndBolt extends BaseRichBolt
{
    public static final String NOTIFICATION_STREAM_NAME = AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
    
    protected Map stormConfig;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }

    @Override
    public void execute(Tuple tuple) 
    {
        StormTaskTuple t = StormTaskTuple.fromStormTuple(tuple);
        
        emitSuccessNotification(t.getTaskId(), t.getFileUrl(), "", "");
        
        outputCollector.ack(tuple);
    }
    
    private void emitSuccessNotification(long taskId, String resource,
            String message, String additionalInformations)
    {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId, 
                resource, NotificationTuple.States.SUCCESS, message, additionalInformations);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    @Override
    public void prepare(Map stormConfig, TopologyContext tc, OutputCollector oc) 
    {
        this.stormConfig = stormConfig;
        this.topologyContext = tc;
        this.outputCollector = oc;
    }
}
