package eu.europeana.cloud.service.dps.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.Map;

public abstract class AbstractDpsBolt extends BaseRichBolt 
{	
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDpsBolt.class);
    
    protected Tuple inputTuple;
    
    protected Map map;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;

    public abstract void execute(StormTaskTuple t);
    public abstract void prepare();

    @Override
    public void execute(Tuple tuple) 
    {
        inputTuple = tuple;
        
        try 
        {
            StormTaskTuple t = StormTaskTuple.fromStormTuple(tuple);
            execute(t);
        } 
        catch (Exception e) 
        {
            LOGGER.error("AbstractDpsBolt error: {} \nStackTrace: \n{}", e.getMessage(), e.getStackTrace());
            
            outputCollector.ack(tuple);
        }
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc)
    {
       this.map = map;
       this.topologyContext = tc;
       this.outputCollector = oc;
       
       prepare();
    }  

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare(new Fields(
                StormTupleKeys.TASK_ID_TUPLE_KEY,
                StormTupleKeys.TASK_NAME_TUPLE_KEY,
                StormTupleKeys.INPUT_FILES_TUPLE_KEY,
                StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
                StormTupleKeys.PARAMETERS_TUPLE_KEY));
    }
}
