package eu.europeana.cloud.service.dps.storm.kafka;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;

/**
 * Create and send {@link DpsTask} to Kafka topic.
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
    
    private Producer<String, DpsTask> producer;
    
    /**
     * Constructor of Kafka producer bolt without additional parameters.
     * @param brokerList broker addresses separated by comma (e.g. localhost:9093,192.168.47.129:9093)
     * @param topic topic name
     * @param taskName new task name
     */
    public KafkaProducerBolt(String brokerList, String topic, String taskName) 
    {
        this.brokerList = brokerList;
        this.topic = topic;
        this.taskName = taskName;
        this.parameters = null;
    }
    
    /**
     * Constructor of Kafka producer bolt with additional parameters.
     * @param brokerList broker addresses separated by comma (e.g. localhost:9093,192.168.47.129:9093)
     * @param topic topic name
     * @param taskName new task name
     * @param parameters additional parameters 
     *                  if value == null: use value from StormTaskTuple
     */
    public KafkaProducerBolt(String brokerList, String topic, String taskName, Map<String, String> parameters) 
    {
        this.brokerList = brokerList;
        this.topic = topic;
        this.taskName = taskName;
        this.parameters = parameters;
    }
    
    @Override
    protected void finalize() throws Throwable 
    {
        if(producer != null)
        {
            producer.close();
        }
        super.finalize();
    }
    
    @Override
    public void execute(StormTaskTuple t) 
    {
        DpsTask msg = new DpsTask();

        msg.setTaskName(taskName);
        msg.addParameter(PluginParameterKeys.FILE_URL, t.getFileUrl());
        msg.addParameter(PluginParameterKeys.FILE_DATA, new String(t.getFileData()));

        //add extension parameters (optional)
        if(parameters != null)
        {
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
        }

        KeyedMessage<String, DpsTask> data = new KeyedMessage<>(topic, msg);
        producer.send(data);
        
        outputCollector.emit(inputTuple, t.toStormTuple());
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", serializer);
        props.put("request.required.acks", "1");    //waiting for acknowledgement

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<>(config);
    }   
}
