package eu.europeana.cloud.integration.usecases;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tarek on 9/21/2016.
 */
public class DatasetHelper {
    private DataSetServiceClient dataSetServiceClient;
    private RecordServiceClient recordServiceClient;
    private UISClient uisClient;
    private CloudId cloudId;
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetHelper.class);

    public DatasetHelper(DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, UISClient uisClient) {
        this.dataSetServiceClient = dataSetServiceClient;
        this.recordServiceClient = recordServiceClient;
        this.uisClient = uisClient;
    }

    public final URI prepareDatasetWithRecordsInside(String providerId, String datasetName, String representationName, int numberOfRecords) throws MCSException, MalformedURLException, CloudException {

        createProviderIdIfNotExists(uisClient, providerId);
        URI uri = dataSetServiceClient.createDataSet(providerId, datasetName, "");
        addRecordsToDataset(numberOfRecords, datasetName, providerId, representationName);
        return uri;


    }

    public final URI prepareEmptyDataset(String providerId, String datasetName) throws CloudException, MCSException {
        createProviderIdIfNotExists(uisClient, providerId);
        return dataSetServiceClient.createDataSet(providerId, datasetName, "");

    }

    public final List<Representation> getRepresentationsInsideDataSetByName(String providerId, String datasetName, String representationName) throws MCSException {
        List<Representation> representationList = new ArrayList<>();
        List<Representation> representations = dataSetServiceClient.getDataSetRepresentations(providerId, datasetName);
        for (Representation representation : representations) {
            if (representationName.equals(representation.getRepresentationName())) {
                representationList.add(representation);
            }
        }
        return representationList;
    }

    public final void assignRepresentationVersionToDataSet(String providerId, String datasetName, String cloudId, String representationName, String version) throws MCSException {
        dataSetServiceClient.assignRepresentationToDataSet(providerId, datasetName, cloudId, representationName, version);

    }

    public final void deleteDataset(String providerId, String datasetName) throws MCSException {
        dataSetServiceClient.deleteDataSet(providerId, datasetName);
    }

    public final String getCloudId() {
        return cloudId.getId();
    }

    public final void grantPermissionToVersion(String cloudId, String representationName, String version, String userName, Permission permission) throws MCSException {
        recordServiceClient.grantPermissionsToVersion(cloudId, representationName, version, userName, permission);
    }


    private void addRecordsToDataset(int numberOfRecords, String datasetName, String providerId, String representationName) throws CloudException, MCSException, MalformedURLException {
        for (int i = 0; i < numberOfRecords; i++) {
            assignNewRecordToDataset(datasetName, providerId, representationName);
        }
    }

    private void assignNewRecordToDataset(String datasetName, String providerId, String representationName) throws CloudException, MCSException, MalformedURLException {
        String cloudId = prepareCloudId(providerId);
        String version = getVersionFromFileUri(representationName, providerId);
        dataSetServiceClient.assignRepresentationToDataSet(providerId, datasetName, cloudId, representationName, version);

    }


    private String prepareCloudId(String providerId) throws CloudException, MCSException {
        cloudId = uisClient.createCloudId(providerId);
        return cloudId.getId();
    }

    private void createProviderIdIfNotExists(UISClient uisClient, String providerId) throws CloudException {
        try {
            uisClient.getDataProvider(providerId);
            LOGGER.info("A provider already existed " + providerId);
        } catch (Exception e) {
            DataProviderProperties dataProviderProperties = new DataProviderProperties();
            uisClient.createProvider(providerId, dataProviderProperties);
            LOGGER.info("A new provider is created " + providerId);
        }
    }

    private String addFileToNewRepresentation(String representationName, String providerId) throws MCSException {
        InputStream inputStream = IOUtils.toInputStream("some test data for my input stream");
        URI uri = recordServiceClient.createRepresentation(cloudId.getId(), representationName, providerId, inputStream, "text/plain");
        return uri.toString();
    }

    private String getVersionFromFileUri(String representationName, String providerId) throws MalformedURLException, MCSException {
        String url = addFileToNewRepresentation(representationName, providerId);
        UrlParser parser = new UrlParser(url);
        return parser.getPart(UrlPart.VERSIONS);
    }


}
