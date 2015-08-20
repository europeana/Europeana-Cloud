package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rits.cloning.Cloner;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ReadDatasetBolt extends AbstractDpsBolt
{
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDatasetBolt.class);
    
    private DataSetServiceClient datasetClient;
    private FileServiceClient fileClient;

    public ReadDatasetBolt(String ecloudMcsAddress, String username, String password) 
    {
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
    }

    @Override
    public void execute(StormTaskTuple t) 
    {
        String providerId = t.getParameter(PluginParameterKeys.PROVIDER_ID);
        String datasetId = t.getParameter(PluginParameterKeys.DATASET_ID);
        
        if(providerId == null || providerId.isEmpty() ||
            datasetId == null || datasetId.isEmpty())
        {
            String message = "No ProviderId or DatasetId for retrieve dataset.";
            LOGGER.warn(message);
            emitDropNotification(t.getTaskId(), t.getFileUrl(), message, t.getParameters().toString());
            emitBasicInfo(t.getTaskId(), 0);
            outputCollector.ack(inputTuple);
            return;
        }
        
        LOGGER.info("Reading dataset with providerId: {} and datasetId: {}", providerId, datasetId);
        
        StormTaskTuple tt;
        int emited = 0;
        
        try 
        {
            List<Representation> dataSetRepresentations = datasetClient.getDataSetRepresentations(providerId, datasetId);
            
            for(Representation representation : dataSetRepresentations)
            { 
                for(File file : representation.getFiles())
                {   
                    if(file == null)
                    {
                        continue;
                    }
                    
                    tt = new Cloner().deepClone(t);  //without cloning every emitted tuple will have the same object!!!
                    
                    tt.addParameter(PluginParameterKeys.CLOUD_ID, representation.getCloudId());
                    tt.addParameter(PluginParameterKeys.REPRESENTATION_NAME, representation.getRepresentationName());
                    tt.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, representation.getVersion());
                    tt.addParameter(PluginParameterKeys.FILE_NAME, file.getFileName());
                    
                    URI uri = fileClient.getFileUri(
                            representation.getCloudId(), 
                            representation.getRepresentationName(), 
                            representation.getVersion(), 
                            file.getFileName());                       

                    String url = uri.toString();    //TODO: why file.contentUrl is null???!!!!!!!
                    tt.setFileUrl(url);

                    InputStream is;
                    try 
                    {
                        is = fileClient.getFile(url);
                    } 
                    catch (RepresentationNotExistsException | FileNotExistsException | 
                            WrongContentRangeException ex) 
                    {
                        LOGGER.warn("Can not retrieve file at {}", url);
                        continue;
                    }

                    tt.setFileData(is);
   
                    outputCollector.emit(inputTuple, tt.toStormTuple()); //TODO: use different taskId for every emit (otherwise suffice only one ack for all emits!!!)
                    emited++;
                }
            }
        } 
        catch (DataSetNotExistsException ex)
        {
            String message = String.format("Dataset for providerId: %s and datasetId: %s not exists.", 
                    providerId, datasetId);
            LOGGER.info(message);
            emitDropNotification(t.getTaskId(), t.getFileUrl(), message, t.getParameters().toString());
            
        }
        catch (MCSException | DriverException | IOException ex) 
        {
            LOGGER.error("ReadDatasetBolt error:" + ex.getMessage());
            emitErrorNotification(t.getTaskId(), t.getFileUrl(), ex.getMessage(), t.getParameters().toString());  
        }
        
        emitBasicInfo(t.getTaskId(), emited);
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {       
        datasetClient = new DataSetServiceClient(ecloudMcsAddress, username, password);
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);
    }  
}
