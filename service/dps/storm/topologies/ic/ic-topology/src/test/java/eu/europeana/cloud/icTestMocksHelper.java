package eu.europeana.cloud;

import backtype.storm.Config;
import backtype.storm.testing.MkClusterParam;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api.ImageConverterServiceImpl;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * @author krystian.
 */
public class icTestMocksHelper {
    protected FileServiceClient fileServiceClient;
    protected ImageConverterServiceImpl imageConverterService;
    protected RecordServiceClient recordServiceClient;
    private static final Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();
    private static Gson gson = new GsonBuilder().create();

    static void prettyPrintJSON(List printIn, String input) {
        String json = prettyGson.toJson(printIn);
        System.out.println("=============================" + input + "=============================\n" + json);
    }

    static String parse(List input) {
        return gson.toJson(input);
    }

    static String parse(String input) {
        return gson.toJson(input);
    }


    static MkClusterParam prepareMKClusterParm() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        int SUPERVISORS = 4;
        mkClusterParam.setSupervisors(SUPERVISORS);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);
        return mkClusterParam;
    }

    protected void dirtyMockZookeeperKS() throws Exception {
        ZookeeperKillService zookeeperKillService = Mockito.mock(ZookeeperKillService.class);
        when(zookeeperKillService.hasKillFlag(anyString(), anyLong())).thenReturn(false);
        PowerMockito.whenNew(ZookeeperKillService.class).withAnyArguments().thenReturn(zookeeperKillService);

    }

    protected void dirtyMockRecordSC() throws Exception {
        recordServiceClient = Mockito.mock(RecordServiceClient.class);
        PowerMockito.whenNew(RecordServiceClient.class).withAnyArguments().thenReturn(recordServiceClient);

    }

    protected void dirtyMockFileSC() throws Exception {
        fileServiceClient = Mockito.mock(FileServiceClient.class);
        PowerMockito.whenNew(FileServiceClient.class).withAnyArguments().thenReturn(fileServiceClient);

    }

    protected void dirtyMockImageCS() throws Exception {
        imageConverterService = Mockito.mock(ImageConverterServiceImpl.class);
        PowerMockito.whenNew(ImageConverterServiceImpl.class).withAnyArguments().thenReturn(imageConverterService);

    }
}
