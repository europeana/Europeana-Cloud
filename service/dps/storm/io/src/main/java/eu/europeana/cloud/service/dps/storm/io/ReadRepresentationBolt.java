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
import org.apache.storm.task.OutputCollector;
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

    /**
     * Should be used only on tests.
     */
    public static ReadRepresentationBolt getTestInstance(String ecloudMcsAddress, OutputCollector outputCollector
    ) {
        ReadRepresentationBolt instance = new ReadRepresentationBolt(ecloudMcsAddress);
        instance.outputCollector = outputCollector;
        return instance;

    }

    @Override
    public void prepare() {

    }

    @Override
    public void execute(StormTaskTuple t) {
        readRepresentationBolt(t);
    }

    private void readRepresentationBolt(StormTaskTuple t) {
        FileServiceClient fileClient = new FileServiceClient(ecloudMcsAddress);
        final String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        final String jsonRepresentation = t.getParameter(PluginParameterKeys.REPRESENTATION);
        t.getParameters().remove(PluginParameterKeys.REPRESENTATION);
        final Representation representation = new Gson().fromJson(jsonRepresentation, new TypeToken<Representation>() {
        }.getType());

        fileClient.useAuthorizationHeader(authorizationHeader);
        if (representation != null) {
            for (File file : representation.getFiles()) {
                final String fileUrl = fileClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName()).toString();
                StormTaskTuple stormTaskTuple = buildNextStormTuple(t, fileUrl);
                outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());

            }
        } else {
            LOGGER.warn("Problem while reading representation");
            emitErrorNotification(t.getTaskId(), null, "Problem while reading representation", "");
        }
    }

    private StormTaskTuple buildNextStormTuple(StormTaskTuple t, String fileUrl) {
        StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);
        stormTaskTuple.getParameters().put(PluginParameterKeys.DPS_TASK_INPUT_DATA, fileUrl);
        return stormTaskTuple;
    }
}
