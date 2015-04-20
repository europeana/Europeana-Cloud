package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bolt that is responsible for store file as a new representation.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class StoreFileAsNewRepresentationBolt extends AbstractDpsBolt 
{
    private final String zkAddress;
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;
    private final String topic;
    private final String taskName;
    private final String brokerList;
    private final String serializer = "eu.europeana.cloud.service.dps.storm.JsonEncoder";

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreFileAsNewRepresentationBolt.class);

    private RecordServiceClient recordService;
    private FileServiceClient fileClient;

    /**
     * Store representation without emiting another DpsTask.
     * @param zkAddress
     * @param ecloudMcsAddress
     * @param username
     * @param password
     */
    public StoreFileAsNewRepresentationBolt(String zkAddress, String ecloudMcsAddress, String username, String password) 
    {
        this(zkAddress, ecloudMcsAddress, username, password, null, null, null);
    }

    /**
     * Store representation with emiting another DpsTask to kafka.
     * @param zkAddress
     * @param ecloudMcsAddress
     * @param username
     * @param password
     * @param brokerList
     * @param topic
     * @param taskName
     */
    public StoreFileAsNewRepresentationBolt(String zkAddress, String ecloudMcsAddress, String username, String password, String brokerList, String topic, String taskName) 
    {
        this.zkAddress = zkAddress;
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
        this.brokerList = brokerList;
        this.topic = topic;
        this.taskName = taskName;
    }

    @Override
    public void execute(StormTaskTuple t) 
    {
        String providerId = t.getParameter(PluginParameterKeys.PROVIDER_ID);
        String cloudId = t.getParameter(PluginParameterKeys.CLOUD_ID);
        String representationName = t.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        String mimeType = t.getParameter(PluginParameterKeys.MIME_TYPE);

        URI representation;
        URI newFileUri;

        try 
        {
            representation = recordService.createRepresentation(cloudId, representationName, providerId);
            newFileUri = fileClient.uploadFile(representation.toString(), new ByteArrayInputStream(t.getFileByteData().getBytes()), mimeType);
        } 
        catch (DriverException ex) 
        {
            LOGGER.warn("Can not upload file because:" + ex.getMessage());
            outputCollector.fail(inputTuple);
            return;
        } 
        catch (MCSException ex) 
        {
            LOGGER.error("StoreFileAsNewRepresentationBolt error:" + ex.getMessage());
            outputCollector.ack(inputTuple);
            return;
        }

        if (brokerList != null && topic != null && taskName != null) 
        {
            emitNewDpsTaskToKafka(newFileUri.toString());
        }

        t.setFileUrl(newFileUri.toString());
        outputCollector.emit(inputTuple, t.toStormTuple());
    }

    @Override
    public void prepare() 
    {
        recordService = new RecordServiceClient(ecloudMcsAddress, username, password);
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);
    }

    /**
     * Send message to kafka topic
     * @param fileUrl file that will be processed
     */
    private void emitNewDpsTaskToKafka(String fileUrl) 
    {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", serializer);
        props.put("request.required.acks", "1");    //waiting for acknowledgement

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, DpsTask> producer = new Producer<>(config);

        DpsTask msg = new DpsTask();

        msg.setTaskName(taskName);
        msg.addParameter(PluginParameterKeys.FILE_URL, fileUrl);

        KeyedMessage<String, DpsTask> data = new KeyedMessage<>(topic, msg);
        producer.send(data);
        producer.close();
    }
}
