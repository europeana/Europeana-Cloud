package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
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
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AddResultToDataSetBolt.class);

    private String ecloudMcsAddress;
    private DataSetServiceClient dataSetServiceClient;

    public AddResultToDataSetBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }

    @Override
    public void prepare() {
        dataSetServiceClient = new DataSetServiceClient(ecloudMcsAddress);
    }

    @Override
    public void execute(StormTaskTuple t) {
        final String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        String resultUrl = t.getParameter(PluginParameterKeys.OUTPUT_URL);
        try {
            List<String> datasets = readDataSetsList(t.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS));
            if (datasets != null) {
                LOGGER.info("Data-sets that will be affected: {}", datasets);
                for (String datasetLocation : datasets) {
                    Representation resultRepresentation = parseResultUrl(resultUrl);
                    DataSet dataSet = parseDataSetURl(datasetLocation);
                    assignRepresentationToDataSet(dataSet, resultRepresentation, authorizationHeader);
                }
            }
            if (t.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) == null)
                emitSuccessNotification(t.getTaskId(), t.getFileUrl(), "", "", resultUrl);
            else
                emitSuccessNotification(t.getTaskId(), t.getFileUrl(), "", "", resultUrl, t.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE), t.getParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
        } catch (MCSException | DriverException e) {
            LOGGER.warn("Error while communicating with MCS {}", e.getMessage());
            emitErrorNotification(t.getTaskId(), resultUrl, e.getMessage(), "The cause of the error is: "+e.getCause());
        } catch (MalformedURLException e) {
            emitErrorNotification(t.getTaskId(), resultUrl, e.getMessage(), "The cause of the error is: "+e.getCause());
        }
    }

    private void assignRepresentationToDataSet(DataSet dataSet, Representation resultRepresentation, String authorizationHeader) throws MCSException, DriverException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                dataSetServiceClient.assignRepresentationToDataSet(
                        dataSet.getProviderId(),
                        dataSet.getId(),
                        resultRepresentation.getCloudId(),
                        resultRepresentation.getRepresentationName(),
                        resultRepresentation.getVersion(),
                        AUTHORIZATION,
                        authorizationHeader);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while assigning record to dataset. Retries left: {}", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while assigning record to dataset.");
                    throw e;
                }
            }
        }
    }


    private List<String> readDataSetsList(String listParameter) {
        if (listParameter == null)
            return null;
        return Arrays.asList(listParameter.split(","));
    }

    private Representation parseResultUrl(String url) throws MalformedURLException {
        UrlParser parser = new UrlParser(url);
        if (parser.isUrlToRepresentationVersionFile()) {
            Representation rep = new Representation();
            rep.setCloudId(parser.getPart(UrlPart.RECORDS));
            rep.setRepresentationName(parser.getPart(UrlPart.REPRESENTATIONS));
            rep.setVersion(parser.getPart(UrlPart.VERSIONS));
            return rep;
        }
        throw new MalformedURLException("The resulted output URL is not formulated correctly");
    }

    private DataSet parseDataSetURl(String url) throws MalformedURLException {
        UrlParser parser = new UrlParser(url);
        if (parser.isUrlToDataset()) {
            DataSet dataSet = new DataSet();
            dataSet.setId(parser.getPart(UrlPart.DATA_SETS));
            dataSet.setProviderId(parser.getPart(UrlPart.DATA_PROVIDERS));
            return dataSet;
        }
        throw new MalformedURLException("The dataSet URL is not formulated correctly");

    }


}
