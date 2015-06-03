package eu.europeana.cloud.service.dps.text.kafka.producers;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.topologies.text.TextStrippingConstants;
import eu.europeana.cloud.service.dps.storm.transform.text.pdf.PdfExtractionMethods;
import eu.europeana.cloud.service.dps.storm.transform.text.oai.OaiExtractionMethods;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ParseFileTaskProducer 
{
    private static final String oaiFile = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/3WDIZUNV3TEJOHJJG7B2T54JTOBVCBF7PH55ZT7HCBEIWSTBVCLA/representations/oai/versions/a4bbd440-f4d2-11e4-9bc7-00163eefc9c8/files/meta.oai";
    private static final String pdfFile = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/KP2T3XNLJNJHDK3JEEVXQZEJ25QPKOLQNP4YTW4ND25V662RIQPA/representations/pdf/versions/ea3ced70-e4e6-11e4-806f-00163eefc9c8/files/test.pdf";
    
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

        msg.setTaskName(PluginParameterKeys.NEW_FILE_MESSAGE);

        msg.addParameter(PluginParameterKeys.EXTRACT_TEXT, "True");
        msg.addParameter(PluginParameterKeys.INDEX_DATA, "True");
        msg.addParameter(PluginParameterKeys.STORE_EXTRACTED_TEXT, "False");
        msg.addParameter(PluginParameterKeys.EXTRACTOR, OaiExtractionMethods.DC.name());
        
        msg.addParameter(PluginParameterKeys.FILE_URL, oaiFile);
        
        //if INDEX_DATA == True
        msg.addParameter(PluginParameterKeys.ELASTICSEARCH_INDEX, "TestIndex1");
        msg.addParameter(PluginParameterKeys.ELASTICSEARCH_TYPE, "TestType1");

        KeyedMessage<String, DpsTask> data = new KeyedMessage<>(TextStrippingConstants.KAFKA_INPUT_TOPIC, msg);
        producer.send(data);
        producer.close();
    }
    
}
