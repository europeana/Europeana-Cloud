package eu.europeana.cloud.service.dps.storm.io;


import com.google.gson.Gson;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.List;

/**
 * @author krystian.
 */
public class ReadDatasetBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDatasetBolt.class);
    private final String ecloudMcsAddress;

    public ReadDatasetBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }

    /**
     * Should be used only on tests.
     */
    public static ReadDatasetBolt getTestInstance(String ecloudMcsAddress, OutputCollector outputCollector
    ) {
        ReadDatasetBolt instance = new ReadDatasetBolt(ecloudMcsAddress);
        instance.outputCollector = outputCollector;
        return instance;

    }

    @Override
    public void prepare() {
    }

    @Override
    public void execute(StormTaskTuple t) {
        DataSetServiceClient datasetClient = new DataSetServiceClient(ecloudMcsAddress);
        final String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        datasetClient.useAuthorizationHeader(authorizationHeader);
        emitSingleRepresentationFromDataSet(t, datasetClient);
    }

    public void emitSingleRepresentationFromDataSet(StormTaskTuple t, DataSetServiceClient dataSetServiceClient) {
        final String dataSetUrl = t.getParameter(PluginParameterKeys.DATASET_URL);
        final String representationName = t.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        t.getParameters().remove(PluginParameterKeys.REPRESENTATION_NAME);
        t.getParameters().remove(PluginParameterKeys.DATASET_URL);
        if (dataSetUrl != null) {
            try {
                final UrlParser urlParser = new UrlParser(dataSetUrl);
                if (urlParser.isUrlToDataset()) {
                    RepresentationIterator iterator = dataSetServiceClient.getRepresentationIterator(urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
                    while (iterator.hasNext()) {
                        Representation representation = iterator.next();
                        if (representationName == null || representation.getRepresentationName().equals(representationName)) {
                            StormTaskTuple next = buildStormTaskTuple(t, representation);
                            outputCollector.emit(inputTuple, next.toStormTuple());
                        }
                    }
                } else {
                    LOGGER.warn("dataset url is not formulated correctly {}", dataSetUrl);
                    emitDropNotification(t.getTaskId(), dataSetUrl, "dataset url is not formulated correctly", "");
                }
            } catch (MalformedURLException ex) {
                LOGGER.error("ReadFileBolt error:" + ex.getMessage());
                emitErrorNotification(t.getTaskId(), dataSetUrl, ex.getMessage(), t.getParameters().toString());
            }
        } else {
            String message = "Missing dataset URL";
            LOGGER.warn(message);
            emitDropNotification(t.getTaskId(), "", message, "");
        }
    }

    private StormTaskTuple buildStormTaskTuple(StormTaskTuple t, Representation representation) {
        StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);
        String RepresentationsJson = new Gson().toJson(representation);
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION, RepresentationsJson);
        return stormTaskTuple;
    }


}











