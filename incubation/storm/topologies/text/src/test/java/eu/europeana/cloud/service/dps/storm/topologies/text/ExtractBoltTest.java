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
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Class for test {@link ExtractBolt}.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AbstractDpsBolt.class)
@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class ExtractBoltTest 
{
    private final String storeStream = "storeStream";
    private final String informStream = "informStream";
    
    private final String pdfFilePath = "/rightTestFile.pdf";
    private final String txtFilePath = "/ascii-file.txt";
    private final String imgFilePath = "/Koala.jpg";
    
    @Test
    public void acksTest() throws Exception
    {
        //--- prepare zookeeper kill service mock
        ZookeeperKillService zooKillMock = Mockito.mock(ZookeeperKillService.class);
        Mockito.when(zooKillMock.hasKillFlag(anyString(), anyLong())).thenReturn(false);       
        PowerMockito.whenNew(ZookeeperKillService.class).withAnyArguments().thenReturn(zooKillMock);
        
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
                    builder.setBolt("extractBolt", new ExtractTextBolt(informStream, storeStream))
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
                        //Waits until topology is idle and 'amt' more tuples have been emitted by spouts
                        Testing.trackedWait(tt, 1, 60000);  //topology, amt, timeout
                    }
                    
                    assertEquals(data.size(), tracker.getNumAcks());
                }
            });
    }
    
    @Test
    public void outputsTest() throws Exception
    {
        //--- prepare zookeeper kill service mock
        ZookeeperKillService zooKillMock = Mockito.mock(ZookeeperKillService.class);
        Mockito.when(zooKillMock.hasKillFlag(anyString(), anyLong())).thenReturn(false);       
        PowerMockito.whenNew(ZookeeperKillService.class).withAnyArguments().thenReturn(zooKillMock);
        
        Testing.withLocalCluster(new TestJob() 
            {
                @Override
                public void run(ILocalCluster cluster) throws IOException 
                {
                    //build topology
                    TopologyBuilder builder = new TopologyBuilder();
                    builder.setSpout("testSpout", new FeederSpout(StormTaskTuple.getFields()));
                    builder.setBolt("extractBolt", new ExtractTextBolt(informStream, storeStream))
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
                    completeTopology.setTimeoutMs(60000);
                    
                    Map result = Testing.completeTopology(cluster, topology, completeTopology);
                    
                    List touplesForStore = Testing.readTuples(result, "extractBolt", storeStream);
                    assertEquals(4, touplesForStore.size());
                    List touplesForInform = Testing.readTuples(result, "extractBolt", informStream);
                    assertEquals(9, touplesForInform.size());  
                    List touplesForNotification = Testing.readTuples(result, "extractBolt", 
                            ExtractTextBolt.NOTIFICATION_STREAM_NAME);
                    assertEquals(37, touplesForNotification.size());
                }
            });
    }
    
    private List<StormTaskTuple> prepareInputData() throws IOException
    {
        List<StormTaskTuple> ret = new ArrayList();
        
        List<InputStream> inputDatas= new ArrayList();
        String[] paths = {pdfFilePath, txtFilePath, imgFilePath};
        for(String path: paths)
        {
            //InputStream is = new ByteArrayInputStream(IOUtils.toByteArray(new FileInputStream(path)));  //fileInputStream not supported reset
            InputStream is = new ByteArrayInputStream(IOUtils.toByteArray(getClass().getResourceAsStream(path)));   //fileInputStream not supported reset
            is.mark(0);
            inputDatas.add(is);   
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
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "txt");  //can read everything (e.g. pdf)
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "False");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "txt");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "pdf");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "fdsa");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "pdf");
        params.add(param); 
        param = new HashMap<>();
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "pdf");
        params.add(param);  
        param = new HashMap<>();
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "xxx");
        param.put(PluginParameterKeys.FILE_FORMATS, "{\"xxx\":\"pdf\"}");
        params.add(param);
        param = new HashMap<>();
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "xxx");
        param.put(PluginParameterKeys.FILE_FORMATS, "{\"xxx\":\"pdf\"}");
        param.put(PluginParameterKeys.EXTRACTORS, "{\"pdf\":\"tika_extractor\"}");
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
