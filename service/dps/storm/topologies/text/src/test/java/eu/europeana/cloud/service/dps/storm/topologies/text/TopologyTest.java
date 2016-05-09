package eu.europeana.cloud.service.dps.storm.topologies.text;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.tuple.Values;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadDatasetBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.StoreFileAsRepresentationBolt;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

import org.junit.runner.RunWith;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, ReadDatasetBolt.class,
        NotificationBolt.class, StoreFileAsRepresentationBolt.class, AbstractDpsBolt.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class TopologyTest {
    private static final String validUriPdf = "ecloud/records/K6/representations/pdf/versions/eac8/files/test.pdf";
    private static final String validUriTxt = "ecloud/records/K6/representations/txt/versions/eac8/files/test.txt";

    @Test
    public void outputsTest() throws Exception {
        prepareMock();

        Testing.withLocalCluster(new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws IOException {
                //build topology
                TextStrippingTopology textStrippingTopology = new TextStrippingTopology(TextStrippingTopology.SpoutType.FEEDER);
                StormTopology topology = textStrippingTopology.buildTopology();

                //topology config
                Config config = new Config();
                config.setNumWorkers(1);
                config.setDebug(true);

                //prepare the mock data
                List<DpsTask> data = prepareInputData();
                MockedSources mockedSources = new MockedSources();
                for (DpsTask task : data) {
                    mockedSources.addMockData("KafkaSpout", new Values(new ObjectMapper().writeValueAsString(task)));
                }

                CompleteTopologyParam completeTopology = new CompleteTopologyParam();
                completeTopology.setMockedSources(mockedSources);
                completeTopology.setStormConf(config);
                completeTopology.setTimeoutMs(60000);

                Map result = Testing.completeTopology(cluster, topology, completeTopology);

                assertEquals(12, Testing.readTuples(result, "ParseDpsTask", "ReadDataset").size());
                assertEquals(12, Testing.readTuples(result, "ParseDpsTask", "ReadFile").size());
                assertEquals(112, Testing.readTuples(result, "ParseDpsTask", //112 = drop + basicInfo (56 wrong massages)
                        AbstractDpsBolt.NOTIFICATION_STREAM_NAME).size());

                assertEquals(16, Testing.readTuples(result, "RetrieveDataset").size());
                assertEquals(6, Testing.readTuples(result, "RetrieveFile").size());
                assertEquals(20, Testing.readTuples(result, "RetrieveDataset", //20 = drop + basicInfo + 4 ok (8 wrong messages from dataset)
                        AbstractDpsBolt.NOTIFICATION_STREAM_NAME).size());
                assertEquals(12, Testing.readTuples(result, "RetrieveFile", //18 = drop + basicInfo + 6 ok(6 wrong messages from file)
                        AbstractDpsBolt.NOTIFICATION_STREAM_NAME).size());

                assertEquals(22, Testing.readTuples(result, "ExtractText").size());
                assertEquals(0, Testing.readTuples(result, "ExtractText",
                        AbstractDpsBolt.NOTIFICATION_STREAM_NAME).size());

                assertEquals(22, Testing.readTuples(result, "StoreNewRepresentation").size());
                assertEquals(0, Testing.readTuples(result, "StoreNewRepresentation",
                        AbstractDpsBolt.NOTIFICATION_STREAM_NAME).size());
            }
        });
    }

    private List<DpsTask> prepareInputData() throws IOException {
        String[] taskNames =
                {
                        PluginParameterKeys.NEW_FILE_MESSAGE,
                        PluginParameterKeys.NEW_DATASET_MESSAGE,
                        "",
                        null
                };

        String[] inputUrls =
                {
                        validUriPdf,
                        validUriTxt,
                        "",
                        null
                };

        List<Map<String, String>> params = new ArrayList();
        Map<String, String> param;

        param = new HashMap<>();
        param.put(PluginParameterKeys.PROVIDER_ID, "ceffa");
        param.put(PluginParameterKeys.DATASET_ID, "ceffa_dataset1");
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "something");
        param.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        params.add(param);
        param = new HashMap<>();
        param.put(PluginParameterKeys.REPRESENTATION_NAME, "something");
        param.put(PluginParameterKeys.STORE_EXTRACTED_TEXT, "False");
        param.put(PluginParameterKeys.INDEX_DATA, "false");
        param.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        params.add(param);
        param = new HashMap<>();
        param.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        params.add(param);
        param = new HashMap<>();
        param.put(PluginParameterKeys.EXTRACT_TEXT, "False");
        params.add(param);
        param = new HashMap<>();
        params.add(param);

        List<DpsTask> ret = new ArrayList();
        DpsTask t;
        for (String taskName : taskNames) {
            for (String url : inputUrls) {
                for (Map<String, String> p : params) {
                    t = new DpsTask(taskName);
                    t.setParameters(new Cloner().deepClone(p));
                    t.addParameter(PluginParameterKeys.FILE_URL, url);
                    t.addParameter(PluginParameterKeys.FILE_DATA, null);
                    t.addParameter(PluginParameterKeys.EXPECTED_SIZE, "1");

                    ret.add(t);
                }
            }
        }

        return ret;
    }

    private void prepareMock() throws Exception {
        List<File> files = new ArrayList();
        files.add(new File("file1", "", "", "", 55, new URI(validUriPdf)));
        files.add(new File("file2", "", "", "", 55, new URI(validUriTxt)));

        List<Representation> reps = new ArrayList();
        reps.add(new Representation("cID", "pdf", "ver", null, new URI("some_uri"), "", files, true, null));
        reps.add(new Representation("cID", "txt", "ver", null, new URI("some_uri"), "", files, false, null));

        //--- dataset client mock
        DataSetServiceClient datasetClientMock = Mockito.mock(DataSetServiceClient.class);
        Mockito.when(datasetClientMock.getDataSetRepresentations(anyString(), anyString())).thenReturn(reps);

        PowerMockito.whenNew(DataSetServiceClient.class).withAnyArguments().thenReturn(datasetClientMock);

        //--- file client mock
        FileServiceClient fileClientMock = Mockito.mock(FileServiceClient.class);
        Mockito.when(fileClientMock.getFile(anyString())).thenAnswer(new Answer<InputStream>() {
            @Override
            public InputStream answer(InvocationOnMock invocation) throws Throwable {
                String path;
                String argument = (String) invocation.getArguments()[0];

                if (argument == null || argument.isEmpty()) {
                    throw new FileNotExistsException();
                }
                if (validUriPdf.equals(argument)) {
                    path = "/rightTestFile.pdf";
                } else if (validUriTxt.equals(argument)) {
                    path = "/ascii-file.txt";
                } else {
                    path = "/ascii-file.txt";
                }

                return new ByteArrayInputStream(org.apache.commons.io.IOUtils.toByteArray(getClass().
                        getResourceAsStream(path)));
            }

        });
        Mockito.when(fileClientMock.getFileUri(anyString(), eq("pdf"), anyString(), anyString())).
                thenReturn(new URI(validUriPdf));
        Mockito.when(fileClientMock.getFileUri(anyString(), eq("txt"), anyString(), anyString())).
                thenReturn(new URI(validUriTxt));
        Mockito.when(fileClientMock.uploadFile(anyString(), any(InputStream.class), anyString())).
                thenReturn(new URI("some_uri"));

        PowerMockito.whenNew(FileServiceClient.class).withAnyArguments().thenReturn(fileClientMock);

        //--- cassandra session mock
        Session cassandraSessionMock = Mockito.mock(Session.class);
        Mockito.when(cassandraSessionMock.execute(any(Statement.class))).thenReturn(null);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        CassandraConnectionProvider cassandraProviderMock = Mockito.mock(CassandraConnectionProvider.class);
        Mockito.when(cassandraProviderMock.getSession()).thenReturn(cassandraSessionMock);
        Mockito.when(cassandraSessionMock.prepare(anyString())).thenReturn(preparedStatement);


        PowerMockito.whenNew(CassandraConnectionProvider.class).withAnyArguments().thenReturn(cassandraProviderMock);

        //--- record client mock
        RecordServiceClient recordClientMock = Mockito.mock(RecordServiceClient.class);
        Mockito.when(recordClientMock.getRepresentations(anyString(), anyString())).thenReturn(reps);

        PowerMockito.whenNew(RecordServiceClient.class).withAnyArguments().thenReturn(recordClientMock);

        //--- zookeeper kill service mock
        ZookeeperKillService zooKillMock = Mockito.mock(ZookeeperKillService.class);
        Mockito.when(zooKillMock.hasKillFlag(anyString(), anyLong())).thenReturn(false);

        PowerMockito.whenNew(ZookeeperKillService.class).withAnyArguments().thenReturn(zooKillMock);
    }
}
