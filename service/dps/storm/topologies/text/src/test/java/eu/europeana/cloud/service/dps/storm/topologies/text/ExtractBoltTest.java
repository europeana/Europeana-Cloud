package eu.europeana.cloud.service.dps.storm.topologies.text;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.AckTracker;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TrackedTopology;
import backtype.storm.topology.TopologyBuilder;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ExtractBoltTest 
{
    private final String storeStream = "storeStream";
    private final String informStream = "informStream";
    
    private final String pdfFilePath = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\rightTestFile.pdf";
    private final String oaiFilePath = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\rightDcTestFile.xml";
    private final String imgFilePath = "C:\\Users\\ceffa\\Desktop\\OU_tmp\\Koala.jpg";

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractBoltTest.class);
 
    @Test
    public void acksTest()
    {
        Testing.withTrackedCluster(new TestJob() 
            {
                @Override
                public void run(ILocalCluster cluster) 
                        throws IOException, AlreadyAliveException, InvalidTopologyException 
                {
                    AckTracker tracker = new AckTracker();
                    FeederSpout spout = new FeederSpout(StormTaskTuple.getFields());
                    spout.setAckFailDelegate(tracker);
                    
                    //build topology
                    TopologyBuilder builder = new TopologyBuilder();
                    builder.setSpout("testSpout", spout);
                    builder.setBolt("extractBolt", new ExtractTextBolt(storeStream, informStream))
                            .shuffleGrouping("testSpout");
                    
                    StormTopology topology = builder.createTopology();
                    
                    TrackedTopology tt = Testing.mkTrackedTopology(cluster, topology);
                                        
                    //topology config
                    Config config = new Config();
                    config.setNumWorkers(1);
                    //config.setDebug(true);      

                    cluster.submitTopology("testTopology", config, tt.getTopology());                                    
                    
                    //prepare test data                    
                    List<StormTaskTuple> data = prepareInputData();
                    
                    for(StormTaskTuple tuple: data)
                    {
                        spout.feed(tuple.toStormTuple());
                        Testing.trackedWait(tt);
                    }
                    
                    assertEquals(data.size(), tracker.getNumAcks());
                }
            });
    }
    
    @Test
    public void outputsTest()
    {
        Testing.withLocalCluster(new TestJob() 
            {
                @Override
                public void run(ILocalCluster cluster) throws IOException 
                {
                    //build topology
                    TopologyBuilder builder = new TopologyBuilder();
                    builder.setSpout("testSpout", new FeederSpout(StormTaskTuple.getFields()));
                    builder.setBolt("extractBolt", new ExtractTextBolt(storeStream, informStream))
                            .shuffleGrouping("testSpout");
                    
                    StormTopology topology = builder.createTopology();

                    //topology config
                    Config config = new Config();
                    config.setNumWorkers(1);
                    config.setDebug(true); 
    
                    //prepare the mock data
                    List<StormTaskTuple> data = prepareInputData();
                    MockedSources mockedSources = new MockedSources();
                    for(StormTaskTuple tuple: data)
                    {
                        mockedSources.addMockData("testSpout", tuple.toStormTuple());
                    }
                    
                    CompleteTopologyParam completeTopology = new CompleteTopologyParam();
                    completeTopology.setMockedSources(mockedSources);
                    completeTopology.setStormConf(config);
                    
                    Map result = Testing.completeTopology(cluster, topology, completeTopology);
                    
                    List touplesForStore = Testing.readTuples(result, "extractBolt", storeStream);
                    assertEquals(3, touplesForStore.size());
                    List touplesForInform = Testing.readTuples(result, "extractBolt", informStream);
                    assertEquals(8, touplesForInform.size());                         
                }
            });
    }
    
    private List<StormTaskTuple> prepareInputData() throws IOException
    {
        List<StormTaskTuple> ret = new ArrayList();
        
        List<InputStream> inputDatas= new ArrayList();
        String[] paths = {pdfFilePath, oaiFilePath, imgFilePath};
        try 
        {
            for(String path: paths)
            {
                InputStream is = new ByteArrayInputStream(IOUtils.toByteArray(new FileInputStream(path)));  //fileInputStream not supported reset
                is.mark(0);
                inputDatas.add(is);   
            }
        } 
        catch (FileNotFoundException ex) 
        {
            LOGGER.error("File is not found! {}", ex.getMessage());
            return ret;
        }       
        byte[] a = {};
        InputStream is = new ByteArrayInputStream(a);  
        is.mark(0);
        inputDatas.add(is); 
        inputDatas.add(null);
        
        List<Map<String, String>> params = new ArrayList();
        Map<String, String> param;
        
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "True");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "pdf");
        params.add(param);      
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "False");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "pdf");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "True");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "oai");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "False");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "oai");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "pdf");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "oai");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "pdf");
        param.put(PluginParameterKeys.EXTRACTOR, "some_extractor");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "oai");
        param.put(PluginParameterKeys.EXTRACTOR, "some_extractor");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "False");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "True");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "true");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "oai");
        params.add(param);
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "fdsa");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "oai");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "oai");
        params.add(param); 
        param = new HashMap<>();
        params.add(param); 

        StormTaskTuple test;
        
        int i= 1;
        for(InputStream input: inputDatas)
        {
            for(Map<String, String> p: params)
            {
                test = new StormTaskTuple(i++, "testTask", "fileUrl", null, new Cloner().deepClone(p));
                test.setFileData(input);
                ret.add(test);
                
                if(input != null)
                {
                    input.reset();
                }
            }
        }
        
        return ret;
    }
}
