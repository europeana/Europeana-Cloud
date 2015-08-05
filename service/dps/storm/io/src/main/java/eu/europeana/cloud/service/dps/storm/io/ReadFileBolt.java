package eu.europeana.cloud.service.dps.storm.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.task.TopologyContext;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.metrics.PersistentCountMetric;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.common.web.ParamConstants;

/**
 */
public class ReadFileBolt extends AbstractDpsBolt 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileBolt.class);

    /** Properties to connect to eCloud */
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;

    private FileServiceClient fileClient;

    public ReadFileBolt(String ecloudMcsAddress, String username, String password) 
    {
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
    }

    @Override
    public void prepare() 
    {
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);
    }

    @Override
    public void execute(StormTaskTuple t) 
    {
        String fileUrl = t.getFileUrl();
        if(fileUrl == null || fileUrl.isEmpty())
        {
            LOGGER.warn("No URL for retrieve file.");
            outputCollector.ack(inputTuple);
            return;
        }
 
        try
        {
            InputStream is = fileClient.getFile(fileUrl);          

            t.setFileData(is);
            
            Map<String, String> parsedUri = FileServiceClient.parseFileUri(fileUrl);
            t.addParameter(PluginParameterKeys.CLOUD_ID, parsedUri.get(ParamConstants.P_CLOUDID));
            t.addParameter(PluginParameterKeys.REPRESENTATION_NAME, parsedUri.get(ParamConstants.P_REPRESENTATIONNAME));
            t.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, parsedUri.get(ParamConstants.P_VER));
            t.addParameter(PluginParameterKeys.FILE_NAME, parsedUri.get(ParamConstants.P_FILENAME));

            outputCollector.emit(inputTuple, t.toStormTuple());
        }
        catch (RepresentationNotExistsException | FileNotExistsException | 
                    WrongContentRangeException ex) 
        {
            LOGGER.warn("Can not retrieve file at {}", fileUrl);
        }
        catch (DriverException | MCSException | IOException ex) 
        {
            LOGGER.error("ReadFileBolt error:" + ex.getMessage());
        }
        
        outputCollector.ack(inputTuple);
    }
}