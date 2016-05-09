package eu.europeana.cloud.service.dps.index.kafka.producers;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.storm.topologies.indexer.IndexerConstants;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexFromAnnotationTaskProducer 
{
    private static final String[] indexers= {"elasticsearch_indexer", "solr_indexer"};
    
    private static final String cloudId = "ITTFODDFVFJELQSCM7ZA5E4LFIX7342Q5Q7N6SBNZATWDGMM5A7A";
    private static final String textData = "Some new data for indexing.";
    
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
        props.put("serializer.class", "eu.europeana.cloud.service.dps.storm.kafka.JsonEncoder");
        props.put("request.required.acks", "1");
                
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, DpsTask> producer = new Producer<>(config);

        DpsTask msg = new DpsTask();

        msg.setTaskName(PluginParameterKeys.NEW_ANNOTATION_MESSAGE);
        
        msg.addParameter(PluginParameterKeys.INDEX_DATA, "True");
        IndexerInformations ii = new IndexerInformations(indexers[0], "index_mlt_4", "mlt4", "192.168.47.129:9300");
        msg.addParameter(PluginParameterKeys.INDEXER, ii.toTaskString());
        msg.addParameter(PluginParameterKeys.FILE_URL, "url to annotation");

        KeyedMessage<String, DpsTask> data = new KeyedMessage<>(IndexerConstants.KAFKA_INPUT_TOPIC, msg);
        producer.send(data);
        producer.close();
    }
    
}
