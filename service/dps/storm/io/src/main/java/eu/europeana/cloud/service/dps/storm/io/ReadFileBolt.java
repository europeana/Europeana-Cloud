package eu.europeana.cloud.service.dps.storm.io;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rits.cloning.Cloner;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dps.DpsTask;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Read file/files from MCS and every file emits as separate {@link StormTaskTuple}.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
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
            //try data from DPS task
            String dpsTaskInputData = t.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA);
            if(dpsTaskInputData != null && !dpsTaskInputData.isEmpty())
            {
                Type type = new TypeToken<Map<String, List<String>>>(){}.getType();
                Map<String, List<String>> fromJson = (Map<String, List<String>>) new Gson().fromJson(dpsTaskInputData, type);
                if(fromJson != null && fromJson.containsKey(DpsTask.FILE_URLS))
                {
                    List<String> files = fromJson.get(DpsTask.FILE_URLS);
                    if(!files.isEmpty())
                    {
                        emitFiles(t, files);
                        outputCollector.ack(inputTuple);
                        return;
                    }
                }
            }
            
            String message = "No URL for retrieve file.";
            LOGGER.warn(message);
            emitDropNotification(t.getTaskId(), "", message, t.getParameters().toString());
            emitBasicInfo(t.getTaskId(), 0);
            outputCollector.ack(inputTuple);
            return;
        }
 
        List <String> files = new ArrayList<>();
        files.add(fileUrl);
        emitFiles(t, files);
        
        outputCollector.ack(inputTuple);
    }
    
    private void emitFiles(StormTaskTuple t, List<String> files)
    {
        emitBasicInfo(t.getTaskId(), files.size());
        StormTaskTuple tt;
        
        for(String file: files)
        {
            tt = new Cloner().deepClone(t);  //without cloning every emitted tuple will have the same object!!!
            
            try
            {
                InputStream is = fileClient.getFile(file);          

                tt.setFileData(is);

                Map<String, String> parsedUri = FileServiceClient.parseFileUri(file);
                tt.addParameter(PluginParameterKeys.CLOUD_ID, parsedUri.get(ParamConstants.P_CLOUDID));
                tt.addParameter(PluginParameterKeys.REPRESENTATION_NAME, parsedUri.get(ParamConstants.P_REPRESENTATIONNAME));
                tt.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, parsedUri.get(ParamConstants.P_VER));
                tt.addParameter(PluginParameterKeys.FILE_NAME, parsedUri.get(ParamConstants.P_FILENAME));

                tt.setFileUrl(file);
                
                outputCollector.emit(inputTuple, tt.toStormTuple());
            }
            catch (RepresentationNotExistsException | FileNotExistsException | 
                        WrongContentRangeException ex) 
            {
                LOGGER.warn("Can not retrieve file at {}", file);
                emitDropNotification(t.getTaskId(), file, "Can not retrieve file", "");
            }
            catch (DriverException | MCSException | IOException ex) 
            {
                LOGGER.error("ReadFileBolt error:" + ex.getMessage());
                emitErrorNotification(t.getTaskId(), file, ex.getMessage(), t.getParameters().toString());
            }
        } 
    }
}