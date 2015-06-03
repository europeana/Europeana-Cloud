package eu.europeana.cloud.service.dps.storm.kafka;

import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

/**
 * Creates tasks ({@link DpsTask}) and sends them to Kafka
 * 
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class KafkaProducerBolt extends AbstractDpsBolt
{
    private final String topic;
    private final String taskName;
    private final String brokerList;
    private final String serializer = "eu.europeana.cloud.service.dps.storm.kafka.JsonEncoder";
    private final Map<String, String> parameters;
    
    public KafkaProducerBolt(String brokerList, String topic, String taskName, Map<String, String> parameters) 
    {
        this.brokerList = brokerList;
        this.topic = topic;
        this.taskName = taskName;
        this.parameters = parameters;
    }
    
    @Override
    public void execute(StormTaskTuple t) 
    {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", serializer);
        props.put("request.required.acks", "1");    //waiting for acknowledgement

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, DpsTask> producer = new Producer<>(config);

        DpsTask msg = new DpsTask();

        msg.setTaskName(taskName);
        msg.addParameter(PluginParameterKeys.FILE_URL, t.getFileUrl());
        msg.addParameter(PluginParameterKeys.FILE_DATA, t.getFileByteData());
        
        //add extension parameters (optional)
        for(Map.Entry<String, String> parameter : parameters.entrySet())
        {
            //if value is null it means that value is in received parameters
            if(parameter.getValue() == null)
            {
                String val = t.getParameter(parameter.getKey());
                if(val != null)
                {
                   msg.addParameter(parameter.getKey(), val); 
                }
            }
            else
            {
                msg.addParameter(parameter.getKey(), parameter.getValue());
            }
        }

        KeyedMessage<String, DpsTask> data = new KeyedMessage<>(topic, msg);
        producer.send(data);
        producer.close(); 
        
        outputCollector.emit(inputTuple, t.toStormTuple());
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {}   
}
