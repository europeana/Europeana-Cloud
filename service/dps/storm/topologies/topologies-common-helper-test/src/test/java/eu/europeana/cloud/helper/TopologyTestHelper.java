package eu.europeana.cloud.helper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.storm.dao.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.*;
import org.apache.storm.Config;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.List;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.test.TestConstants.NUM_WORKERS;
import static org.mockito.Matchers.*;

public class TopologyTestHelper {
    protected CassandraTaskInfoDAO taskInfoDAO;
    protected TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
    protected CassandraSubTaskInfoDAO subTaskInfoDAO;
    protected CassandraTaskErrorsDAO taskErrorsDAO;
    protected TaskStatusChecker taskStatusChecker;
    protected TaskStatusUpdater taskStatusUpdater;

    protected FileServiceClient fileServiceClient;
    protected DataSetServiceClient dataSetClient;
    protected RecordServiceClient recordServiceClient;
    protected RevisionServiceClient revisionServiceClient;
    protected ProcessedRecordsDAO processedRecordsDAO;
    private HarvestedRecordsDAO harvestedRecordsDAO;

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
        PowerMockito.when(CassandraTaskInfoDAO.getInstance(any(CassandraConnectionProvider.class))).thenReturn(taskInfoDAO);

        taskDiagnosticInfoDAO = Mockito.mock(TaskDiagnosticInfoDAO.class);
        PowerMockito.mockStatic(TaskDiagnosticInfoDAO.class);
        PowerMockito.when(TaskDiagnosticInfoDAO.getInstance(any(CassandraConnectionProvider.class))).thenReturn(taskDiagnosticInfoDAO);

        taskStatusChecker = Mockito.mock(TaskStatusChecker.class);
        PowerMockito.mockStatic(TaskStatusChecker.class);
        PowerMockito.when(TaskStatusChecker.getTaskStatusChecker()).thenReturn(taskStatusChecker);

        taskStatusUpdater = Mockito.mock(TaskStatusUpdater.class);
        PowerMockito.mockStatic(TaskStatusUpdater.class);
        PowerMockito.when(TaskStatusUpdater.getInstance(any(CassandraConnectionProvider.class))).thenReturn(taskStatusUpdater);

        subTaskInfoDAO = Mockito.mock(CassandraSubTaskInfoDAO.class);
        PowerMockito.mockStatic(CassandraSubTaskInfoDAO.class);
        PowerMockito.when(CassandraSubTaskInfoDAO.getInstance(any(CassandraConnectionProvider.class))).thenReturn(subTaskInfoDAO);

        processedRecordsDAO = Mockito.mock(ProcessedRecordsDAO.class);
        PowerMockito.mockStatic(ProcessedRecordsDAO.class);
        PowerMockito.when(ProcessedRecordsDAO.getInstance(any(CassandraConnectionProvider.class))).thenReturn(processedRecordsDAO);

        taskErrorsDAO = Mockito.mock(CassandraTaskErrorsDAO.class);
        PowerMockito.mockStatic(CassandraTaskErrorsDAO.class);
        PowerMockito.when(CassandraTaskErrorsDAO.getInstance(any(CassandraConnectionProvider.class))).thenReturn(taskErrorsDAO);

        harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);
        PowerMockito.mockStatic(HarvestedRecordsDAO.class);
        PowerMockito.when(HarvestedRecordsDAO.getInstance(any(CassandraConnectionProvider.class))).thenReturn(harvestedRecordsDAO);

        PowerMockito.when(taskInfoDAO.isDroppedTask(anyLong())).thenReturn(false);
        PowerMockito.mockStatic(CassandraConnectionProviderSingleton.class);
        PowerMockito.when(CassandraConnectionProviderSingleton.getCassandraConnectionProvider(anyString(), anyInt(), anyString(), anyString(), anyString())).thenReturn(Mockito.mock(CassandraConnectionProvider.class));
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
        conf.put(CASSANDRA_SECRET_TOKEN, CASSANDRA_SECRET_TOKEN);
//        conf.put(Config.SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS,1000000);
//        conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS,1000000);
        conf.setNumAckers(0);
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
