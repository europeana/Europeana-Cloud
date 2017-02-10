package eu.europeana.cloud.integration.usecases;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

import static eu.europeana.cloud.integration.helper.IntegrationConstants.*;

/**
 * Created by Tarek on 9/19/2016.
 */

public class CreateDatasetFromDatasetOfAnotherProviderTestCase implements TestCase {

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


    public void executeTestCase() throws CloudException, MCSException, IOException {
        System.out.println("CreateDatasetFromDatasetOfAnotherProviderTestCase started ..");
        try {
            String now = TestHelper.getTime();
            prepareTestCase();
            List<CloudVersionRevisionResponse> cloudVersionRevisionResponseList = destinationDatasetHelper.getDataSetCloudIdsByRepresentation(DESTINATION_DATASET_NAME, DESTINATION_PROVIDER_ID, SOURCE_REPRESENTATION_NAME, now, Tags.PUBLISHED.getTag());
            assertCloudVersionRevisionResponseListWithExpectedValues(cloudVersionRevisionResponseList);
            System.out.println("CreateDatasetFromDatasetOfAnotherProviderTestCase Finished Successfully!");
        } finally {
            cleanUp();
        }
    }

    private void assertCloudVersionRevisionResponseListWithExpectedValues(List<CloudVersionRevisionResponse> cloudVersionRevisionResponseList) throws MCSException {
        assertNotNull(cloudVersionRevisionResponseList);
        assertEquals(cloudVersionRevisionResponseList.size(), RECORDS_NUMBERS * 2);
        int sourceVersionsCount = 0;
        int destinationVersionCount = 0;
        String sourceRevisionId = RevisionUtils.getRevisionKey(SOURCE_PROVIDER_ID, SOURCE_REVISION_NAME);
        String destinationRevisionId = RevisionUtils.getRevisionKey(DESTINATION_PROVIDER_ID, DESTINATION_REVISION_NAME);
        for (CloudVersionRevisionResponse cloudVersionRevisionResponse : cloudVersionRevisionResponseList) {
            if (sourceRevisionId.equals(cloudVersionRevisionResponse.getRevisionId()))
                sourceVersionsCount++;
            else if (destinationRevisionId.equals(cloudVersionRevisionResponse.getRevisionId()))
                destinationVersionCount++;
        }
        assertEquals(sourceVersionsCount, 3);
        assertEquals(destinationVersionCount, 3);
    }

    private void prepareTestCase() throws MCSException, MalformedURLException, CloudException {
        List<String> tags = new ArrayList<>();
        tags.add(Tags.PUBLISHED.getTag());
        tags.add(Tags.DELETED.getTag());
        sourceDatasetHelper.prepareDatasetWithRecordsInside(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, SOURCE_REVISION_NAME, tags, RECORDS_NUMBERS, null);
        destinationDatasetHelper.prepareEmptyDataset(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME);
        List<Representation> representations = sourceDatasetHelper.getRepresentationsInsideDataSetByName(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME);
        for (Representation representation : representations) {
            destinationDatasetHelper.assignRepresentationVersionToDataSet(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME, representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());
            sourceDatasetHelper.grantPermissionToVersion(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), appProperties.getProperty("destinationUserName"), Permission.READ);
            revisionServiceClient.addRevision(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), DESTINATION_REVISION_NAME, DESTINATION_PROVIDER_ID, Tags.PUBLISHED.getTag());
        }
    }

    public void cleanUp() throws CloudException, MCSException {
        System.out.println("CreateDatasetFromDatasetOfAnotherProviderTestCase cleaning up ..");
        sourceDatasetHelper.deleteDataset(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME);
        destinationDatasetHelper.deleteDataset(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME);
        Set<String> cloudIds = sourceDatasetHelper.getCloudIds();
        for (String cloudId : cloudIds) {
            adminRecordServiceClient.deleteRecord(cloudId);
            adminUisClient.deleteCloudId(cloudId);
        }
        sourceDatasetHelper.cleanCloudIds();

    }

}
