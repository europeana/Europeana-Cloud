package eu.europeana.cloud.integration.usecases;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.integration.helper.IntegrationConstants;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Tarek on 2/9/2017.
 */
public class ActiveRecordsTestCase extends IntegrationConstants implements TestCase {

    @Resource
    private DatasetHelper sourceDatasetHelper;

    @Resource
    private UISClient adminUisClient;

    @Resource
    private RecordServiceClient adminRecordServiceClient;

    private final static int RECORDS_NUMBERS = 3;

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateDatasetTestCase.class);


    public void executeTestCase() throws CloudException, MCSException, IOException {
        System.out.println("ActiveRecordsTestCase started ..");
        try {
            List<String> tags = new ArrayList<>();
            tags.add(Tags.PUBLISHED.getTag());
            tags.add(Tags.DELETED.getTag());
            prepareTestCase(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, DEREFERENCE_REVISION, tags, RECORDS_NUMBERS, null);
            List<CloudIdAndTimestampResponse> dereferenceCloudIdAndTimestampResponseList = sourceDatasetHelper.getLatestDataSetCloudIdByRepresentationAndRevision(SOURCE_DATASET_NAME, SOURCE_PROVIDER_ID, SOURCE_PROVIDER_ID, DEREFERENCE_REVISION, SOURCE_REPRESENTATION_NAME, null);
            assertNotNull(dereferenceCloudIdAndTimestampResponseList);
            assertEquals(dereferenceCloudIdAndTimestampResponseList.size(), RECORDS_NUMBERS);
            tags = new ArrayList<>();
            tags.add(Tags.PUBLISHED.getTag());
            Set<String> cloudIds = sourceDatasetHelper.getCloudIds();
            prepareTestCase(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, PUBLISH_REVISION, tags, RECORDS_NUMBERS, cloudIds.iterator().next());
            List<CloudIdAndTimestampResponse> publishedCloudIdAndTimestampResponseList = sourceDatasetHelper.getLatestDataSetCloudIdByRepresentationAndRevision(SOURCE_DATASET_NAME, SOURCE_PROVIDER_ID, SOURCE_PROVIDER_ID, PUBLISH_REVISION, SOURCE_REPRESENTATION_NAME, false);
            assertNotNull(publishedCloudIdAndTimestampResponseList);
            assertEquals(publishedCloudIdAndTimestampResponseList.size(), 1);

            List<CloudIdAndTimestampResponse> intersectedCloudIdAndTimestamps = intersectCloudIdAndTimestampResponsesBasedOnCloudId(dereferenceCloudIdAndTimestampResponseList, publishedCloudIdAndTimestampResponseList);
            assertNotNull(intersectedCloudIdAndTimestamps);
            assertEquals(intersectedCloudIdAndTimestamps.size(), 1);
            System.out.println("ActiveRecordsTestCase Finished Successfully ..");


          /*
            assertNotNull(cloudVersionRevisionResponseList);
            assertEquals(cloudVersionRevisionResponseList.size(), RECORDS_NUMBERS);
            String newDate = TestHelper.getTime();
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
            */
        } finally {
            cleanUp();
        }
    }

    private List<CloudIdAndTimestampResponse> intersectCloudIdAndTimestampResponsesBasedOnCloudId(List<CloudIdAndTimestampResponse> dereferenceCloudIdAndTimestampResponseList, List<CloudIdAndTimestampResponse> publishedCloudIdAndTimestampResponseList) {
        List<CloudIdAndTimestampResponse> intesectCloudIds = new ArrayList<>();
        for (CloudIdAndTimestampResponse dereferenceCloudIdAndTimestamp : dereferenceCloudIdAndTimestampResponseList) {
            String dereferenceCloudId = dereferenceCloudIdAndTimestamp.getCloudId();
            for (CloudIdAndTimestampResponse publishedCloudIdAndTimestamp : publishedCloudIdAndTimestampResponseList) {
                String publishCloudId = publishedCloudIdAndTimestamp.getCloudId();
                if (dereferenceCloudId.equals(publishCloudId)) {
                    intesectCloudIds.add(dereferenceCloudIdAndTimestamp);
                }
            }
        }
        return intesectCloudIds;
    }


    private void prepareTestCase(String providerId, String datasetName, String representationName, String revisionName, List<String> tags, int recordNum, String cloudId) throws MCSException, MalformedURLException, CloudException {
        sourceDatasetHelper.prepareDatasetWithRecordsInside(providerId, datasetName, representationName, revisionName, tags, recordNum, cloudId);
    }

    public void cleanUp() throws CloudException, MCSException {
        Set<String> cloudIds = sourceDatasetHelper.getCloudIds();
        for (String cloudId : cloudIds) {
            adminRecordServiceClient.deleteRepresentation(cloudId, SOURCE_REPRESENTATION_NAME);
            adminUisClient.deleteCloudId(cloudId);
        }
        try {
            sourceDatasetHelper.deleteDataset(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME);
        } catch (DataSetNotExistsException e) {
            LOGGER.info("The source dataSet {} can't be removed because it doesn't exist ", SOURCE_DATASET_NAME);

        }

    }
}

