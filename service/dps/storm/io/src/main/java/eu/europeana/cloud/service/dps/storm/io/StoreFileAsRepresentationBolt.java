package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.net.URI;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store data as representation.
 * If nonpersistent version already exists then store file to this version. Otherwise create new version.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class StoreFileAsRepresentationBolt extends AbstractDpsBolt 
{
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreFileAsRepresentationBolt.class);

    private RecordServiceClient recordService;
    private FileServiceClient fileClient;

    /**
     * Store representation.
     * @param ecloudMcsAddress MCS API URL
     * @param username eCloud username
     * @param password eCloud password
     */
    public StoreFileAsRepresentationBolt(String ecloudMcsAddress, String username, String password) 
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
        String mimeType = t.getParameter(PluginParameterKeys.MIME_TYPE);
        String representationName = t.getParameter(PluginParameterKeys.REPRESENTATION_NAME);      

        URI representation = null;
        URI newFileUri;

        try 
        {
            try
            {
                List<Representation> representations = recordService.getRepresentations(cloudId, representationName);
                
                //find nonpersistent version
                for(Representation rep: representations)
                {
                    if(!rep.isPersistent())
                    {
                        representation = rep.getUri();
                        break;
                    }
                }
                
                //create new verion if all versions are persistent
                if(representation == null)
                {
                    representation = recordService.createRepresentation(cloudId, representationName, providerId);
                }
            }
            catch(RepresentationNotExistsException e)
            {
                representation = recordService.createRepresentation(cloudId, representationName, providerId);
            }
            
            newFileUri = fileClient.uploadFile(representation.toString(), t.getFileByteDataAsStream(), mimeType);
        } 
        catch (DriverException ex) 
        {
            String message = "Can not upload file because:" + ex.getMessage();
            LOGGER.warn(message);
            emitErrorNotification(t.getTaskId(), t.getFileUrl(), message, "");
            outputCollector.fail(inputTuple);
            return;
        } 
        catch (MCSException ex) 
        {
            LOGGER.error("StoreFileAsNewRepresentationBolt error:" + ex.getMessage());
            emitErrorNotification(t.getTaskId(), t.getFileUrl(), ex.getMessage(), t.getParameters().toString());
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
