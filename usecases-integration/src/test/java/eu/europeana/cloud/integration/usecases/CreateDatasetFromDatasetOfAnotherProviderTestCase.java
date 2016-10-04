package eu.europeana.cloud.integration.usecases;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.integration.helper.IntegrationConstants;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * Created by Tarek on 9/19/2016.
 */

public class CreateDatasetFromDatasetOfAnotherProviderTestCase extends IntegrationConstants implements TestCase {
    @Resource
    private DatasetHelper sourceDatasetHelper;
    @Resource
    private DatasetHelper destinationDatasetHelper;

    @Resource
    private RecordServiceClient adminRecordServiceClient;

    @Resource
    private UISClient adminUisClient;

    @Resource
    private Properties appProperties;


    private final static int RECORDS_NUMBERS = 3;


    @Autowired
    private RevisionServiceClient revisionServiceClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDatasetFromDatasetOfAnotherProviderTestCase.class);


    public void executeTestCase() throws CloudException, MCSException, IOException {
        try {
            prepareTestCase();
        } finally {
            cleanUp();
        }
    }

    private void prepareTestCase() throws MCSException, MalformedURLException, CloudException {
        URI uri = sourceDatasetHelper.prepareDatasetWithRecordsInside(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, SOURCE_REVISION_NAME, Tags.PUBLISHED.getTag(), RECORDS_NUMBERS);
        LOGGER.info("The source dataSet {} has been created! It is url is {}", SOURCE_DATASET_NAME, uri);
        destinationDatasetHelper.prepareEmptyDataset(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME);
        LOGGER.info("The destination dataSet {} has been created! It is url is {} ", DESTINATION_DATASET_NAME, uri);
        List<Representation> representations = sourceDatasetHelper.getRepresentationsInsideDataSetByName(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME);
        for (Representation representation : representations) {
            destinationDatasetHelper.assignRepresentationVersionToDataSet(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME, representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());
            sourceDatasetHelper.grantPermissionToVersion(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), appProperties.getProperty("destinationUserName"), Permission.READ);
            revisionServiceClient.addRevision(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), DESTINATION_REVISION_NAME, DESTINATION_PROVIDER_ID, Tags.PUBLISHED.getTag());
        }
    }

    public void cleanUp() throws CloudException, MCSException {
        try {
            sourceDatasetHelper.deleteDataset(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME);
        } catch (DataSetNotExistsException e) {

        }
        try {
            destinationDatasetHelper.deleteDataset(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME);
        } catch (DataSetNotExistsException e) {

        }
        List<String> cloudIds = sourceDatasetHelper.getCloudIds();
        for (String cloudId : cloudIds) {
            adminRecordServiceClient.deleteRecord(cloudId);
            adminUisClient.deleteCloudId(cloudId);
        }

    }
}
