package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class AddResultToDataSetBolt extends AbstractDpsBolt {

    private String mcsLocation;
    private transient DataSetServiceClient dataSetClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(AddResultToDataSetBolt.class);

    public AddResultToDataSetBolt(String mcsLocation) {
        this.mcsLocation = mcsLocation;
    }

    @Override
    public void execute(StormTaskTuple t) {
        LOGGER.info("AddResultToDataSetBolt execution started");
        initDataSetClient(t);
        addRepresentationToDataSets(t);
        emitSuccess(t);
    }

    private void initDataSetClient(StormTaskTuple t) {
        LOGGER.debug("Initializing dataSet client");
        if (dataSetClient == null) {
            dataSetClient = new DataSetServiceClient(mcsLocation);
            dataSetClient.useAuthorizationHeader(t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
        }
    }

    public void addRepresentationToDataSets(StormTaskTuple t) {

        String resultUrl = t.getParameter(PluginParameterKeys.OUTPUT_URL);

        LOGGER.info("Resulting resource to insert: " + resultUrl);
        
        try {
            List<String> datasets = readDataSetsList(t.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS));
            LOGGER.info("Data-sets that will be affected: " + datasets);
            
            for (String datasetLocation : datasets) {
                Representation resultRepresentation = parseResultUrl(resultUrl);
                DataSet dataSet = parseDataSetURl(datasetLocation);
                dataSetClient.assignRepresentationToDataSet(
                        dataSet.getProviderId(),
                        dataSet.getId(),
                        resultRepresentation.getCloudId(),
                        resultRepresentation.getRepresentationName(),
                        resultRepresentation.getVersion());
            }
            emitSuccessNotification(t.getTaskId(), t.getFileUrl(), "", "", resultUrl.toString());

        } catch (MCSException e) {
            emitErrorNotification(t.getTaskId(), resultUrl, e.getMessage(), t.getParameters().toString());
            e.printStackTrace();
        } catch (MalformedURLException e) {
            emitErrorNotification(t.getTaskId(), resultUrl, e.getMessage(), t.getParameters().toString());
            e.printStackTrace();
        } catch (NullPointerException e) {
            emitErrorNotification(t.getTaskId(), resultUrl, e.getMessage(), t.getParameters().toString());
            e.printStackTrace();
        }
    }

    @Override
    public void prepare() {

    }

    private List<String> readDataSetsList(String listParameter) {
        return Arrays.asList(listParameter.split(","));
    }

    private Representation parseResultUrl(String url) throws MalformedURLException {
        UrlParser parser = new UrlParser(url);
        if (parser.isUrlToRepresentationVersionFile()) {
            Representation rep = new Representation();
            rep.setCloudId(parser.getPart(UrlPart.RECORDS));
            rep.setRepresentationName(parser.getPart(UrlPart.REPRESENTATIONS));
            rep.setVersion(parser.getPart(UrlPart.VERSIONS));
            parser.getPart(UrlPart.FILES);
            return rep;
        }
        return null;
    }

    private DataSet parseDataSetURl(String url) throws MalformedURLException {
        DataSet dataSet = null;
        UrlParser parser = new UrlParser(url);
        if (parser.isUrlToDataset()) {
            dataSet = new DataSet();
            dataSet.setId(parser.getPart(UrlPart.DATA_SETS));
            dataSet.setProviderId(parser.getPart(UrlPart.DATA_PROVIDERS));
        }
        return dataSet;
    }

    private void emitSuccessNotification(long taskId, String resource,
                                         String message, String additionalInformations, String resultResource) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.SUCCESS, message, additionalInformations, resultResource);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }
}
