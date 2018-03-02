package eu.europeana.cloud.integration.usecases;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.io.IOException;
import java.net.MalformedURLException;

import static eu.europeana.cloud.integration.usecases.IntegrationConstants.*;
import static org.junit.Assert.*;

/**
 * Created by Tarek on 9/19/2016.
 */

public class UpdateDatasetTestCase implements TestCase {


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
    private RevisionServiceClient destinationRevisionServiceClient;


    public void executeTestCase() throws CloudException, MCSException, IOException {
        System.out.println("UpdateDatasetTestCase started ..");
        try {
            String now = TestHelper.getCurrentTime();
            prepareTestCase();
            List<CloudVersionRevisionResponse> cloudVersionRevisionResponseList = sourceDatasetHelper.getDataSetCloudIdsByRepresentation(SOURCE_DATASET_NAME, SOURCE_PROVIDER_ID, SOURCE_REPRESENTATION_NAME, now, Tags.PUBLISHED.getTag());
            assertNotNull(cloudVersionRevisionResponseList);
            assertEquals(cloudVersionRevisionResponseList.size(), RECORDS_NUMBERS);
            String newDate = TestHelper.getCurrentTime();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (CloudVersionRevisionResponse cloudVersionRevisionResponse : cloudVersionRevisionResponseList) {
                destinationDatasetHelper.assignRepresentationVersionToDataSet(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME, cloudVersionRevisionResponse.getCloudId(), SOURCE_REPRESENTATION_NAME, cloudVersionRevisionResponse.getVersion());
                sourceDatasetHelper.grantPermissionToVersion(cloudVersionRevisionResponse.getCloudId(), SOURCE_REPRESENTATION_NAME, cloudVersionRevisionResponse.getVersion(), appProperties.getProperty("destinationUserName"), Permission.READ);
                HashSet<Tags> tags = new HashSet<>();
                if (cloudVersionRevisionResponse.isDeleted())
                    tags.add(Tags.DELETED);
                destinationRevisionServiceClient.addRevision(cloudVersionRevisionResponse.getCloudId(), SOURCE_REPRESENTATION_NAME, cloudVersionRevisionResponse.getVersion(), DESTINATION_REVISION_NAME, DESTINATION_PROVIDER_ID, tags);
            }
            List<CloudVersionRevisionResponse> destinationCloudVersionRevisionResponse = destinationDatasetHelper.getDataSetCloudIdsByRepresentation(DESTINATION_DATASET_NAME, DESTINATION_PROVIDER_ID, SOURCE_REPRESENTATION_NAME, newDate, Tags.PUBLISHED.getTag());
            assertEquals(destinationCloudVersionRevisionResponse.size(), 0);
            System.out.println("UpdateDatasetTestCase Finished Successfully!");
        } finally {
            cleanUp();
        }
    }


    private void prepareTestCase() throws MCSException, MalformedURLException, CloudException {
        List<String> tags = new ArrayList<>();
        tags.add(Tags.PUBLISHED.getTag());
        tags.add(Tags.DELETED.getTag());
        sourceDatasetHelper.prepareDatasetWithRecordsInside(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, SOURCE_REVISION_NAME, tags, RECORDS_NUMBERS, null);
        destinationDatasetHelper.prepareEmptyDataset(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME);

    }

    public void cleanUp() throws CloudException, MCSException {
        System.out.println("UpdateDatasetTestCase cleaning up ..");
        Set<String> cloudIds = sourceDatasetHelper.getCloudIds();
        for (String cloudId : cloudIds) {
            adminRecordServiceClient.deleteRepresentation(cloudId, SOURCE_REPRESENTATION_NAME);
            adminUisClient.deleteCloudId(cloudId);
        }
        sourceDatasetHelper.deleteDataset(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME);
        destinationDatasetHelper.deleteDataset(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME);
        sourceDatasetHelper.cleanCloudIds();



    }

}
