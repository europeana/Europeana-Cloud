package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.PersistentCountMetric;
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
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.rits.cloning.Cloner;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ReadDatasetBolt extends AbstractDpsBolt
{
    private final String zkAddress;
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDatasetBolt.class);
    
    private DataSetServiceClient datasetClient;
    private FileServiceClient fileClient;
    
    private transient CountMetric countMetric;
    private transient PersistentCountMetric pCountMetric;
    private transient ReducedMetric wordLengthMeanMetric;

    public ReadDatasetBolt(String zkAddress, String ecloudMcsAddress, String username, String password) 
    {
        this.zkAddress = zkAddress;
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
            LOGGER.warn("No ProviderId or DatasetId for retrieve dataset.");
            outputCollector.ack(inputTuple);
            return;
        }
        
        LOGGER.info("Reading dataset with providerId: {} and datasetId: {}", providerId, datasetId);
        
        StormTaskTuple tt;
        
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
                        LOGGER.info("Can not retrieve file at {}", url);
                        continue;
                    }

                    tt.setFileData(is);

                    updateMetrics(tt, IOUtils.toString(is));
   
                    outputCollector.emit(inputTuple, tt.toStormTuple()); //TODO: use different taskId for every emit (otherwise suffice only one ack for all emits!!!)
                }
            }           
            outputCollector.ack(inputTuple);
        } 
        catch (DataSetNotExistsException ex)
        {
            LOGGER.info("Dataset for providerId: {} and datasetId: {} not exists.", providerId, datasetId);
            outputCollector.ack(inputTuple);
        }
        catch (MCSException | DriverException | IOException ex) 
        {
            LOGGER.error("ReadDatasetBolt error:" + ex.getMessage());
            outputCollector.ack(inputTuple);   
        }
    }

    @Override
    public void prepare() 
    {       
        datasetClient = new DataSetServiceClient(ecloudMcsAddress, username, password);
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);
        
        initMetrics(topologyContext);
    }
    
    void initMetrics(TopologyContext context) 
    {
        countMetric = new CountMetric();
        pCountMetric = new PersistentCountMetric();
        wordLengthMeanMetric = new ReducedMetric(new MeanReducer());

        context.registerMetric("read_records=>", countMetric, 10);
        context.registerMetric("pCountMetric_records=>", pCountMetric, 10);
        context.registerMetric("word_length=>", wordLengthMeanMetric, 10);
    }
    
    void updateMetrics(StormTaskTuple t, String word) 
    {		
        countMetric.incr();
        pCountMetric.incr();
        wordLengthMeanMetric.update(word.length());
    }   
}
