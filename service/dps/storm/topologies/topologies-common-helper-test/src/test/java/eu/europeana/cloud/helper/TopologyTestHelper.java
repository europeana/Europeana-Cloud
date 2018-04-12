package eu.europeana.cloud.helper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.storm.Config;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.List;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_PASSWORD;
import static eu.europeana.cloud.service.dps.test.TestConstants.NUM_WORKERS;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 11/8/2017.
 */
public class TopologyTestHelper {
    protected CassandraTaskInfoDAO taskInfoDAO;
    protected CassandraSubTaskInfoDAO subTaskInfoDAO;
    protected CassandraTaskErrorsDAO taskErrorsDAO;
    protected TaskStatusChecker taskStatusChecker;

    protected FileServiceClient fileServiceClient;
    protected DataSetServiceClient dataSetClient;
    protected RecordServiceClient recordServiceClient;
    protected RevisionServiceClient revisionServiceClient;

    protected static MkClusterParam prepareMKClusterParm() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        int SUPERVISORS = 4;
        mkClusterParam.setSupervisors(SUPERVISORS);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);
        return mkClusterParam;
    }

    protected void mockCassandraInteraction() throws Exception {
        taskInfoDAO = Mockito.mock(CassandraTaskInfoDAO.class);
        PowerMockito.mockStatic(CassandraTaskInfoDAO.class);
        when(CassandraTaskInfoDAO.getInstance(isA(CassandraConnectionProvider.class))).thenReturn(taskInfoDAO);

        taskStatusChecker = Mockito.mock(TaskStatusChecker.class);
        PowerMockito.mockStatic(TaskStatusChecker.class);
        when(TaskStatusChecker.getTaskStatusChecker()).thenReturn(taskStatusChecker);

        subTaskInfoDAO = Mockito.mock(CassandraSubTaskInfoDAO.class);
        PowerMockito.mockStatic(CassandraSubTaskInfoDAO.class);
        when(CassandraSubTaskInfoDAO.getInstance(isA(CassandraConnectionProvider.class))).thenReturn(subTaskInfoDAO);
        taskErrorsDAO = Mockito.mock(CassandraTaskErrorsDAO.class);
        PowerMockito.mockStatic(CassandraTaskErrorsDAO.class);
        when(CassandraTaskErrorsDAO.getInstance(isA(CassandraConnectionProvider.class))).thenReturn(taskErrorsDAO);
        when(taskInfoDAO.hasKillFlag(anyLong())).thenReturn(false);
        PowerMockito.mockStatic(CassandraConnectionProviderSingleton.class);
        when(CassandraConnectionProviderSingleton.getCassandraConnectionProvider(anyString(), anyInt(), anyString(), anyString(), anyString())).thenReturn(Mockito.mock(CassandraConnectionProvider.class));
    }


    protected void mockRecordSC() throws Exception {
        recordServiceClient = Mockito.mock(RecordServiceClient.class);
        PowerMockito.whenNew(RecordServiceClient.class).withAnyArguments().thenReturn(recordServiceClient);

    }

    protected void mockFileSC() throws Exception {
        fileServiceClient = Mockito.mock(FileServiceClient.class);
        PowerMockito.whenNew(FileServiceClient.class).withAnyArguments().thenReturn(fileServiceClient);

    }

    protected void mockDatSetClient() throws Exception {
        dataSetClient = Mockito.mock(DataSetServiceClient.class);
        PowerMockito.whenNew(DataSetServiceClient.class).withAnyArguments().thenReturn(dataSetClient);
    }

    protected void mockRevisionServiceClient() throws Exception {
        revisionServiceClient = Mockito.mock(RevisionServiceClient.class);
        PowerMockito.whenNew(RevisionServiceClient.class).withAnyArguments().thenReturn(revisionServiceClient);

    }

    protected CompleteTopologyParam prepareCompleteTopologyParam(MockedSources mockedSources) {

        CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
        completeTopologyParam.setMockedSources(mockedSources);
        completeTopologyParam.setStormConf(buildConfig());
        return completeTopologyParam;
    }

    private Config buildConfig() {
        Config conf = new Config();
        conf.setNumWorkers(NUM_WORKERS);
        conf.put(CASSANDRA_HOSTS, CASSANDRA_HOSTS);
        conf.put(CASSANDRA_PORT, "9042");
        conf.put(CASSANDRA_KEYSPACE_NAME, CASSANDRA_KEYSPACE_NAME);
        conf.put(CASSANDRA_USERNAME, CASSANDRA_USERNAME);
        conf.put(CASSANDRA_PASSWORD, CASSANDRA_PASSWORD);
        return conf;
    }


    private static final Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();
    private static Gson gson = new GsonBuilder().create();

    protected static void prettyPrintJSON(List printIn, String input) {
        String json = prettyGson.toJson(printIn);
        System.out.println("=============================" + input + "=============================\n" + json);
    }

    protected static String parse(List input) {
        return gson.toJson(input);
    }
}
