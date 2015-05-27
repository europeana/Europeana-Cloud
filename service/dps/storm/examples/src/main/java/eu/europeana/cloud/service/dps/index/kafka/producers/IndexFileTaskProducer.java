package eu.europeana.cloud.service.dps.index.kafka.producers;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.topologies.indexer.IndexerConstants;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexFileTaskProducer 
{
    private static final String txtFile = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/ITTFODDFVFJELQSCM7ZA5E4LFIX7342Q5Q7N6SBNZATWDGMM5A7A/representations/txt/versions/d7664c80-f4d3-11e4-9bc7-00163eefc9c8/files/9634129_text_v1.txt";
    
    /**
     * @param args the command line arguments
     *      1) kafka broker
     */
    public static void main(String[] args) 
    {
        Properties props = new Properties();
        if(args.length >= 1)
        {
            props.put("metadata.broker.list", args[0]);
        }
        else
        {
            props.put("metadata.broker.list", "192.168.47.129:9093");
        } 
        props.put("serializer.class", "eu.europeana.cloud.service.dps.storm.JsonEncoder");
        props.put("request.required.acks", "1");
                
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, DpsTask> producer = new Producer<>(config);

        DpsTask msg = new DpsTask();

        msg.setTaskName(PluginParameterKeys.INDEX_FILE_MESSAGE);
        
        msg.addParameter(PluginParameterKeys.INDEX_DATA, "True");
        msg.addParameter(PluginParameterKeys.ELASTICSEARCH_INDEX, "test_index1");
        msg.addParameter(PluginParameterKeys.ELASTICSEARCH_TYPE, "test_type1");
        
        msg.addParameter(PluginParameterKeys.FILE_URL, txtFile);

        KeyedMessage<String, DpsTask> data = new KeyedMessage<>(IndexerConstants.KAFKA_INPUT_TOPIC, msg);
        producer.send(data);
        producer.close();
    }
    
}
