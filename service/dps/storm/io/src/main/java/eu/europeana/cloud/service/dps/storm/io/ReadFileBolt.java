package eu.europeana.cloud.service.dps.storm.io;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.PersistentCountMetric;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import java.io.IOException;
import java.util.Map;

/**
 */
public class ReadFileBolt extends AbstractDpsBolt 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileBolt.class);

    /** Properties to connect to eCloud */
    private final String zkAddress;
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;

    private transient CountMetric countMetric;
    private transient PersistentCountMetric pCountMetric;

    private FileServiceClient fileClient;

    public ReadFileBolt(String zkAddress, String ecloudMcsAddress, String username,String password) 
    {
        this.zkAddress = zkAddress;
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
    }

    @Override
    public void prepare() 
    {
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);

        initMetrics(topologyContext);
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
            t.addParameter(PluginParameterKeys.CLOUD_ID, parsedUri.get("CLOUDID"));
            t.addParameter(PluginParameterKeys.REPRESENTATION_NAME, parsedUri.get("REPRESENTATIONNAME"));
            t.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, parsedUri.get("VERSION"));
            t.addParameter(PluginParameterKeys.FILE_NAME, parsedUri.get("FILENAME"));

            updateMetrics(t, IOUtils.toString(is));

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

    void initMetrics(TopologyContext context) 
    {
        countMetric = new CountMetric();
        pCountMetric = new PersistentCountMetric();

        context.registerMetric("read_records=>", countMetric, 10);
        context.registerMetric("pCountMetric_records=>", pCountMetric, 10);
    }
    
    void updateMetrics(StormTaskTuple t, String word) 
    {
        countMetric.incr();
        pCountMetric.incr();
        LOGGER.info("ReadFileBolt: metrics updated");
    }
}