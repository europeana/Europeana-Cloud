package eu.europeana.cloud.service.dps.storm.topologies.text;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.StormTupleKeys;
import java.util.Map;

public class ParseTaskBolt extends BaseBasicBolt 
{
    public static final Logger LOGGER = LoggerFactory.getLogger(ParseTaskBolt.class);
    
    public final Map<String, String> routingRules;

    /**
     * 
     * @param routingRules 
     */
    public ParseTaskBolt(Map<String, String> routingRules) 
    {
        this.routingRules = routingRules;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        for(Map.Entry<String, String> rule : routingRules.entrySet())
        {
            declarer.declareStream(rule.getValue(), new Fields(
                StormTupleKeys.TASK_ID_TUPLE_KEY,
                StormTupleKeys.TASK_NAME_TUPLE_KEY,
                StormTupleKeys.INPUT_FILES_TUPLE_KEY,
                StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
                StormTupleKeys.PARAMETERS_TUPLE_KEY));
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) 
    {
        ObjectMapper mapper = new ObjectMapper();
        DpsTask task = null;
        try 
        {
            task = mapper.readValue(tuple.getString(0), DpsTask.class);
        } 
        catch (IOException e) 
        {
            LOGGER.error("Message '{}' rejected because: {}", tuple.getString(0), e.getMessage());
            return;
        }

        HashMap<String, String> taskParameters = task.getParameters();
        //HashMap<String, List<String>> inputData = task.getInputData();

        StormTaskTuple stormTaskTuple = new StormTaskTuple(
                task.getTaskId(), 
                task.getTaskName(), 
                null, null, taskParameters);
        
        if(taskParameters != null)
        {
            stormTaskTuple.setFileUrl(taskParameters.get(PluginParameterKeys.FILE_URL));
            stormTaskTuple.setFileData(taskParameters.get(PluginParameterKeys.FILE_DATA));
            
            LOGGER.info("taskParameters size=" + taskParameters.size());
        }
        
        if(routingRules.containsKey(task.getTaskName()))
        {
            collector.emit(routingRules.get(task.getTaskName()), stormTaskTuple.toStormTuple());
        }
    }
}
