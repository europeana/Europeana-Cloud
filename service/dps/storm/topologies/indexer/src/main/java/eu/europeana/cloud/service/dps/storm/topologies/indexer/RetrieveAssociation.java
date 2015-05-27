package eu.europeana.cloud.service.dps.storm.topologies.indexer;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class RetrieveAssociation extends AbstractDpsBolt
{
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RetrieveAssociation.class);
    
    private FileServiceClient fileClient;
    private RecordServiceClient recordClient;
    
    private final List<String> supportedRepresentations;
    
    public RetrieveAssociation(String ecloudMcsAddress, String username, String password) 
    {
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
        
        supportedRepresentations = new ArrayList<>();
        supportedRepresentations.add("oai");
    }

    @Override
    public void execute(StormTaskTuple t) 
    {
        InputStream fileContent = null;
        
        try 
        {
            String cloudId = t.getParameter(PluginParameterKeys.CLOUD_ID);
            if(cloudId == null)
            {
                LOGGER.warn("Missing cloudId in task {}", t.getTaskId());
                outputCollector.ack(inputTuple);
                return;
            }
            
            Record record = recordClient.getRecord(cloudId);
                        
            //find one of the supported representation and retrieve content from first file
            for(Representation rep: record.getRepresentations())
            {
                if(supportedRepresentations.contains(rep.getRepresentationName()))
                {
                    File f = rep.getFiles().get(0);
                    if(f != null)
                    {
                        fileContent = fileClient.getFile(f.getContentUri().toString());
                        break;
                    }
                }
            }
            
            if(fileContent != null)
            {
                t.addParameter(PluginParameterKeys.METADATA, IOUtils.toString(fileContent));
                
                outputCollector.emit(inputTuple, t.toStormTuple());
            }
        } 
        catch (MCSException | IOException ex) 
        {   
            LOGGER.warn("Problem with retrieving metadata content in task {} because: {}", t.getTaskId(), ex.getMessage());
        }
               
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);
        recordClient = new RecordServiceClient(ecloudMcsAddress, username, password);
    }
    
}
