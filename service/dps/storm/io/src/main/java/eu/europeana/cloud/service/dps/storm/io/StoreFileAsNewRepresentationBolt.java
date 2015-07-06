package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bolt that is responsible for store file as a new representation.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class StoreFileAsNewRepresentationBolt extends AbstractDpsBolt 
{
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreFileAsNewRepresentationBolt.class);

    private RecordServiceClient recordService;
    private FileServiceClient fileClient;

    /**
     * Store representation.
     * @param ecloudMcsAddress
     * @param username
     * @param password
     */
    public StoreFileAsNewRepresentationBolt(String ecloudMcsAddress, String username, String password) 
    {
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
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
            newFileUri = fileClient.uploadFile(representation.toString(), t.getFileByteDataAsStream(), mimeType);
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

        t.setFileUrl(newFileUri.toString());
        outputCollector.emit(inputTuple, t.toStormTuple());
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {
        recordService = new RecordServiceClient(ecloudMcsAddress, username, password);
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);
    }
}
