package eu.europeana.cloud.service.dps.storm.topologies.text;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.tuple.Values;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TopologyTest 
{        
	/**
	 * DISABLED: @see ECL-522 (https://jira.man.poznan.pl/jira/browse/ECL-522)
	 */
//    @Test
    public void outputsTest()
    {
        Testing.withLocalCluster(new TestJob() 
            {
                @Override
                public void run(ILocalCluster cluster) throws IOException 
                {
                    //build topology
                    TextStrippingTopology textStrippingTopology = new TextStrippingTopology(TextStrippingTopology.SpoutType.KAFKA);
                    StormTopology topology = textStrippingTopology.buildTopology();
                    
                    //topology config
                    Config config = new Config();
                    config.setNumWorkers(1);
                    config.setDebug(true); 
                    //config.registerMetricsConsumer(LoggingMetricsConsumer.class);
                    //config.put(Config.STORM_LOCAL_DIR, "C:\\");
    
                    //prepare the mock data                 
                    List<DpsTask> data = prepareInputData();
                    MockedSources mockedSources = new MockedSources();
                    for(DpsTask task: data)
                    {
                        mockedSources.addMockData("KafkaSpout", new Values(new ObjectMapper().writeValueAsString(task)));
                    }
                    
                    CompleteTopologyParam completeTopology = new CompleteTopologyParam();
                    completeTopology.setMockedSources(mockedSources);
                    completeTopology.setStormConf(config);
                    
                    Map result = Testing.completeTopology(cluster, topology, completeTopology);

                    assertEquals(24, Testing.readTuples(result, "ParseDpsTask", "ReadDataset").size());
                    assertEquals(24, Testing.readTuples(result, "ParseDpsTask", "ReadFile").size());
                    assertEquals(6, Testing.readTuples(result, "ExtractText", "StoreStream").size());
                    assertEquals(18, Testing.readTuples(result, "ExtractText", "InformStream").size());
                    assertEquals(6, Testing.readTuples(result, "StoreNewRepresentation").size());
                    assertEquals(24, Testing.readTuples(result, "InformBolt").size());     
                }
            });
    }
    
    private List<DpsTask> prepareInputData() throws IOException
    {
        String[] taskNames = 
        {
            PluginParameterKeys.NEW_FILE_MESSAGE,
            PluginParameterKeys.NEW_DATASET_MESSAGE,            
            "",
            null
        };
        
        String[] inputUrls =
        {
            "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/KP2T3XNLJNJHDK3JEEVXQZEJ25QPKOLQNP4YTW4ND25V662RIQPA/representations/pdf/versions/ea3ced70-e4e6-11e4-806f-00163eefc9c8/files/test.pdf",
            "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/3WDIZUNV3TEJOHJJG7B2T54JTOBVCBF7PH55ZT7HCBEIWSTBVCLA/representations/oai/versions/a4bbd440-f4d2-11e4-9bc7-00163eefc9c8/files/meta.oai",
            "",
            null
        }; 

        List<Map<String, String>> params = new ArrayList();
        Map<String, String> param;
        
        param = new HashMap<>();
        param.put(PluginParameterKeys.PROVIDER_ID, "ceffa");
        param.put(PluginParameterKeys.DATASET_ID, "ceffa_dataset1");
        param.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.PROVIDER_ID, "ceffa");
        param.put(PluginParameterKeys.DATASET_ID, "ceffa_dataset1");
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "True");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "something");
        param.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        params.add(param);
        param = new HashMap<>();
        param.put(PluginParameterKeys.PROVIDER_ID, "ceffa");
        param.put(PluginParameterKeys.DATASET_ID, "ceffa_dataset1");
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "False");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "something");
        param.put(PluginParameterKeys.INDEX_DATA, "True");
        param.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        params.add(param);
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "fff");
        param.put(PluginParameterKeys.INDEX_DATA, "false");
        param.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        params.add(param);
        param = new HashMap<>();
        param.put(PluginParameterKeys.EXTRACTOR, "some extractor");
        param.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        params.add(param);
        param = new HashMap<>();
        param.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        params.add(param);
        param = new HashMap<>();
        params.add(param);
        
        List<DpsTask> ret = new ArrayList(); 
        DpsTask t;
        for(String taskName: taskNames)
        {
            for(String url: inputUrls)
            {
                for(Map<String, String> p: params)
                {
                    t = new DpsTask(taskName);
                    t.setParameters(new Cloner().deepClone(p));
                    t.addParameter(PluginParameterKeys.FILE_URL, url);
                    t.addParameter(PluginParameterKeys.FILE_DATA, null);
                    
                    ret.add(t);
                }
            }
        }
        
        return ret;
    }
}
