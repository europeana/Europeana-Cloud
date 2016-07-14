package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.task.OutputCollector;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * @author krystian.
 */
public class ReadDataSetBolt extends AbstractDpsBolt{


    private DataSetServiceClient datasetClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDataSetBolt.class);
    private final String ecloudMcsAddress;

    public ReadDataSetBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }

    /**
     * Should be used only on tests.
     */
    public static ReadDataSetBolt getTestInstance(String ecloudMcsAddress, OutputCollector outputCollector,
                                                   DataSetServiceClient datasetClient) {
        ReadDataSetBolt instance = new ReadDataSetBolt(ecloudMcsAddress);
        instance.outputCollector = outputCollector;
        instance.datasetClient = datasetClient;
        return instance;

    }

    @Override
    public void prepare() {
        datasetClient = new DataSetServiceClient(ecloudMcsAddress);
    }

    @Override
    public void execute(StormTaskTuple t) {
        emitSingleRepresentationFromDataSet(t);
    }

    private void emitSingleRepresentationFromDataSet(StormTaskTuple t) {
        final String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        final String dataSetUrls = t.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA);
        final String representationName = t.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        final Map<String, List<String>> inputDpsTaskData = new Gson().fromJson(dataSetUrls, new TypeToken<Map<String, List<String>>>() {
        }.getType());
        final List<String> dataSet = inputDpsTaskData.get(DpsTask.DATASET_URLS);
        datasetClient.useAuthorizationHeader(authorizationHeader);


        if (dataSet.size() == 1) {
            final String dataSetUrl = dataSet.get(0);
            try {
                final UrlParser urlParser = new UrlParser(dataSetUrl);
                if (urlParser.isUrlToDataset()) {
                    List<Representation> representations = datasetClient.getDataSetRepresentations(urlParser.getPart(UrlPart.DATA_PROVIDERS),
                            urlParser.getPart(UrlPart.DATA_SETS));

                    for (Representation representation : representations) {
                        if (representationName == null || representation.getRepresentationName().equals(representationName)) {

                            StormTaskTuple next = buildStormTaskTuple(t, representation);
                            outputCollector.emit(inputTuple,next.toStormTuple());

                        }
                    }
                } else {
                    LOGGER.warn("dataset url is not formulated correctly {}", inputDpsTaskData);
                    emitDropNotification(t.getTaskId(), dataSetUrl, "dataset url is not formulated correctly", "");
                }
            } catch (DataSetNotExistsException ex) {
                LOGGER.warn("Provided dataset is not existed {}", inputDpsTaskData);
                emitDropNotification(t.getTaskId(), dataSetUrl, "Can not retrieve a dataset", "");
            } catch (MalformedURLException | MCSException ex) {
                LOGGER.error("ReadFileBolt error:" + ex.getMessage());
                emitErrorNotification(t.getTaskId(), dataSetUrl, ex.getMessage(), t.getParameters().toString());
            }
        } else {
            LOGGER.warn("more than one dataset url {}", inputDpsTaskData);
            emitDropNotification(t.getTaskId(), new Gson().toJson(dataSet), "more than one dataset url {}", "");
        }


    }

    private StormTaskTuple buildStormTaskTuple(StormTaskTuple t, Representation representation) {
        StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);
        String RepresentationsJson = new Gson().toJson(Arrays.asList(representation));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION, RepresentationsJson);
        stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, null);
        return stormTaskTuple;
    }


}
