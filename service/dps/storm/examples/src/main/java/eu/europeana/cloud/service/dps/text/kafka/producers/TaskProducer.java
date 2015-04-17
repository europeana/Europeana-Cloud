package eu.europeana.cloud.service.dps.text.kafka.producers;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.transform.text.ExtractionMethods;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TaskProducer 
{
    public static final String datasetId = "ceffa_dataset1";
    public static final String providerId = "ceffa";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) 
    {
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.47.129:9093");
        props.put("serializer.class", "eu.europeana.cloud.service.dps.storm.JsonEncoder");
        props.put("request.required.acks", "1");
                
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, DpsTask> producer = new Producer<String, DpsTask>(config);

        String key = "dataset_for_extraction";
        DpsTask msg = new DpsTask();

        msg.setTaskName("Text extraction for dataset");

        msg.addParameter(PluginParameterKeys.PROVIDER_ID, providerId);
        msg.addParameter(PluginParameterKeys.DATASET_ID, datasetId);
        msg.addParameter(PluginParameterKeys.EXTRACTOR, ExtractionMethods.TIKA_EXTRACTOR.name());

        KeyedMessage<String, DpsTask> data = new KeyedMessage<String, DpsTask>(
                "text_stripping", key, msg);
        producer.send(data);
        producer.close();
    }
    
}
