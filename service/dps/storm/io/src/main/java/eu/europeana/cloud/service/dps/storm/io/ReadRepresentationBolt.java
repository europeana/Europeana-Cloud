package eu.europeana.cloud.service.dps.storm.io;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author krystian.
 */
public class ReadRepresentationBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadRepresentationBolt.class);
    private final String ecloudMcsAddress;


    public ReadRepresentationBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }


    @Override
    public void prepare() {

    }

    @Override
    public void execute(StormTaskTuple t) {
        FileServiceClient fileClient = new FileServiceClient(ecloudMcsAddress);
        final String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        fileClient.useAuthorizationHeader(authorizationHeader);
        readRepresentationBolt(fileClient, t);
    }

    void readRepresentationBolt(FileServiceClient fileClient, StormTaskTuple t) {
        final String jsonRepresentation = t.getParameter(PluginParameterKeys.REPRESENTATION);
        t.getParameters().remove(PluginParameterKeys.REPRESENTATION);
        final Representation representation = new Gson().fromJson(jsonRepresentation, new TypeToken<Representation>() {
        }.getType());


        if (representation != null) {
            for (File file : representation.getFiles()) {
                if (!taskStatusChecker.hasKillFlag(t.getTaskId())) {
                    try {
                        final String fileUrl = getFileUri(fileClient, representation, file);
                        StormTaskTuple stormTaskTuple = buildNextStormTuple(t, fileUrl);
                        outputCollector.emit(stormTaskTuple.toStormTuple());
                    } catch (Exception e) {
                        LOGGER.warn("Error while getting File URI from MCS {}", e.getMessage());
                        emitErrorNotification(t.getTaskId(), file.getFileName(), "Error while getting File URI from MCS" + e.getMessage(), t.getParameters().toString());

                    }
                } else
                    break;
            }
        } else {
            LOGGER.warn("Problem while reading representation");
            emitErrorNotification(t.getTaskId(), null, "Problem while reading representation", "");
        }
    }

    private String getFileUri(FileServiceClient fileClient, Representation representation, File file) throws Exception {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return fileClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName()).toString();
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting a file URI. Retries left:{} ", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting a file URI.");
                    throw e;
                }
            }
        }
    }

    private StormTaskTuple buildNextStormTuple(StormTaskTuple t, String fileUrl) {
        StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);
        stormTaskTuple.getParameters().put(PluginParameterKeys.DPS_TASK_INPUT_DATA, fileUrl);
        return stormTaskTuple;
    }
}
