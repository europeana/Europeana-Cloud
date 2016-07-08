package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.task.OutputCollector;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author krystian.
 */
public class ReadRepresentationBolt extends AbstractDpsBolt{

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadRepresentationBolt.class);
    private final String ecloudMcsAddress;
    private FileServiceClient fileClient;

    public ReadRepresentationBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }

    /**
     * Should be used only on tests.
     */
    public static ReadRepresentationBolt getTestInstance(String ecloudMcsAddress, OutputCollector outputCollector,
                                                         FileServiceClient fileClient) {
        ReadRepresentationBolt instance = new ReadRepresentationBolt(ecloudMcsAddress);
        instance.outputCollector = outputCollector;
        instance.fileClient = fileClient;
        return instance;

    }

    @Override
    public void prepare() {
        fileClient = new FileServiceClient(ecloudMcsAddress);
    }

    @Override
    public void execute(StormTaskTuple t) {
        readRepresentationBolt(t);
    }

    private void readRepresentationBolt(StormTaskTuple t) {
        final String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        final String representationsUrls = t.getParameter(PluginParameterKeys.REPRESENTATION);
        final List<Representation> representations = new Gson().fromJson(representationsUrls, new TypeToken<List<Representation>>() {
        }.getType());

        fileClient.useAuthorizationHeader(authorizationHeader);

        if(representations.size() == 1) {
            Representation representation = representations.get(0);
            for(File file : representation.getFiles()) {
                final String fileUrl = fileClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName()).toString();

                StormTaskTuple next = buildStormTaskTuple(t, fileUrl);
                outputCollector.emit(inputTuple,next.toStormTuple());

            }
        } else {
            LOGGER.warn("more than one representation url {}", representationsUrls);
            emitDropNotification(t.getTaskId(), representationsUrls, "more than one representation url {}", "");
        }
    }

    private StormTaskTuple buildStormTaskTuple(StormTaskTuple t, String fileUrl) {
        StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);
        Map<String, List<String>> dpsTaskInputData = new HashMap<>();
        dpsTaskInputData.put(DpsTask.FILE_URLS, Arrays.asList(fileUrl));
        stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, new Gson().toJson(dpsTaskInputData));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION, null);
        return stormTaskTuple;
    }

}
