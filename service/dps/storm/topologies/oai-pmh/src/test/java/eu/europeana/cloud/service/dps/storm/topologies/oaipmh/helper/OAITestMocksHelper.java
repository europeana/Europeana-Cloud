package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.mcs.driver.*;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.OAIClientProvider;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.SourceProvider;
import org.apache.storm.Config;
import org.apache.storm.testing.MkClusterParam;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.List;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

/**
 * @author krystian.
 */
public class OAITestMocksHelper {
    protected FileServiceClient fileServiceClient;
    protected DataSetServiceClient dataSetClient;
    protected RecordServiceClient recordServiceClient;
    protected RevisionServiceClient revisionServiceClient;
    protected SourceProvider sourceProvider;
    protected OAIClientProvider oaiClientProvider;
    protected UISClient uisClient;


    private static final Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();
    private static Gson gson = new GsonBuilder().create();

    protected static void prettyPrintJSON(List printIn, String input) {
        String json = prettyGson.toJson(printIn);
        System.out.println("=============================" + input + "=============================\n" + json);
    }

    protected static String parse(List input) {
        return gson.toJson(input);
    }

    protected static MkClusterParam prepareMKClusterParm() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        int SUPERVISORS = 4;
        mkClusterParam.setSupervisors(SUPERVISORS);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);
        return mkClusterParam;
    }

    protected void mockZookeeperKS() throws Exception {
        ZookeeperKillService zookeeperKillService = Mockito.mock(ZookeeperKillService.class);
        when(zookeeperKillService.hasKillFlag(anyString(), anyLong())).thenReturn(false);
        PowerMockito.whenNew(ZookeeperKillService.class).withAnyArguments().thenReturn(zookeeperKillService);

    }

    protected void mockRecordSC() throws Exception {
        recordServiceClient = Mockito.mock(RecordServiceClient.class);
        PowerMockito.whenNew(RecordServiceClient.class).withAnyArguments().thenReturn(recordServiceClient);

    }

    protected void mockSourceProvider() throws Exception {
        sourceProvider = Mockito.mock(SourceProvider.class);
        PowerMockito.whenNew(SourceProvider.class).withAnyArguments().thenReturn(sourceProvider);

    }

    protected void mockUISClient() throws Exception {
        uisClient = Mockito.mock(UISClient.class);
        PowerMockito.whenNew(UISClient.class).withAnyArguments().thenReturn(uisClient);

    }

    protected void mockOAIClientProvider() throws Exception {
        oaiClientProvider = Mockito.mock(OAIClientProvider.class);
        PowerMockito.whenNew(OAIClientProvider.class).withAnyArguments().thenReturn(oaiClientProvider);

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
}
