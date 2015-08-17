package eu.europeana.cloud.service.dps.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

public abstract class AbstractDpsBolt extends BaseRichBolt 
{	
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDpsBolt.class);
    
    public static final String NOTIFICATION_STREAM_NAME = "NotificationStream";
    
    protected Tuple inputTuple;
    
    protected Map stormConfig;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;

    public abstract void execute(StormTaskTuple t);
    public abstract void prepare();

    @Override
    public void execute(Tuple tuple) 
    {
        inputTuple = tuple;
        
        StormTaskTuple t = null;
        try 
        {
            t = StormTaskTuple.fromStormTuple(tuple);
            execute(t);
        } 
        catch (Exception e) 
        {
            LOGGER.error("AbstractDpsBolt error: {} \nStackTrace: \n{}", e.getMessage(), e.getStackTrace());
            
            if(t != null)
            {
                StringWriter stack = new StringWriter();
                e.printStackTrace(new PrintWriter(stack));
                emitErrorNotification(t.getTaskId(), t.getFileUrl(), e.getMessage(), stack.toString());
            }
            outputCollector.ack(tuple);
        }
    }

    @Override
    public void prepare(Map stormConfig, TopologyContext tc, OutputCollector oc)
    {
       this.stormConfig = stormConfig;
       this.topologyContext = tc;
       this.outputCollector = oc;
       
       prepare();
    }  

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        //default stream
        declarer.declare(StormTaskTuple.getFields());
        
        //notifications
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }
    
    protected void emitDropNotification(long taskId, String resource, String message, String additionalInformations)
    {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId, 
                resource, NotificationTuple.States.DROPPED, message, additionalInformations);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }
    
    protected void emitSuccessNotification(long taskId, String resource, String message, String additionalInformations)
    {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId, 
                resource, NotificationTuple.States.SUCCESS, message, additionalInformations);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }
    
    protected void emitErrorNotification(long taskId, String resource, String message, String additionalInformations)
    {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId, 
                resource, NotificationTuple.States.ERROR, message, additionalInformations);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }
    
    protected void emitBasicInfo(long taskId, int expectedSize)
    {
        NotificationTuple nt = NotificationTuple.prepareBasicInfo(taskId, expectedSize);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }
}
