package eu.europeana.cloud.integration.usecases;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.integration.helper.IntegrationConstants;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

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
            String now = getNow();
            prepareTestCase();
            List<CloudVersionRevisionResponse> cloudVersionRevisionResponseList = destinationDatasetHelper.getDataSetCloudIdsByRepresentation(DESTINATION_DATASET_NAME, DESTINATION_PROVIDER_ID, SOURCE_REPRESENTATION_NAME, now, Tags.PUBLISHED.getTag());
            assertCloudVersionRevisionResponseListWithExpectedValues(cloudVersionRevisionResponseList);
        } finally {
            cleanUp();
        }
    }

    private void assertCloudVersionRevisionResponseListWithExpectedValues(List<CloudVersionRevisionResponse> cloudVersionRevisionResponseList) throws MCSException {
        assertNotNull(cloudVersionRevisionResponseList);
        assertEquals(cloudVersionRevisionResponseList.size(), RECORDS_NUMBERS * 2);
        int sourceVersionsCount = 0;
        int destinationVersionCount = 0;

        List<Representation> representations = sourceDatasetHelper.getRepresentationsInsideDataSetByName(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME);
        List<String> sourceRevisionIds = new ArrayList<>();
        List<String> destinationRevisionIds = new ArrayList<>();

        for (Representation representation : representations) {
            for (Revision revision : representation.getRevisions()) {
                if (SOURCE_PROVIDER_ID.equals(revision.getRevisionProviderId()) && SOURCE_REVISION_NAME.equals(revision.getRevisionName()))
                    sourceRevisionIds.add(RevisionUtils.getRevisionKey(revision));
                else if (DESTINATION_PROVIDER_ID.equals(revision.getRevisionProviderId()) && DESTINATION_REVISION_NAME.equals(revision.getRevisionName()))
                    destinationRevisionIds.add(RevisionUtils.getRevisionKey(revision));
            }
        }
        for (CloudVersionRevisionResponse cloudVersionRevisionResponse : cloudVersionRevisionResponseList) {
            if (sourceRevisionIds.contains(cloudVersionRevisionResponse.getRevisionId()))
                sourceVersionsCount++;
            else if (destinationRevisionIds.contains(cloudVersionRevisionResponse.getRevisionId()))
                destinationVersionCount++;
        }
        assertEquals(sourceVersionsCount, 3);
        assertEquals(destinationVersionCount, 3);
    }

    private void prepareTestCase() throws MCSException, MalformedURLException, CloudException {
        URI uri = sourceDatasetHelper.prepareDatasetWithRecordsInside(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, SOURCE_REVISION_NAME, Tags.PUBLISHED.getTag(), RECORDS_NUMBERS);
        LOGGER.info("The source dataSet {} has been created! Its url is {}", SOURCE_DATASET_NAME, uri);
        destinationDatasetHelper.prepareEmptyDataset(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME);
        LOGGER.info("The destination dataSet {} has been created! Its url is {} ", DESTINATION_DATASET_NAME, uri);
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
            LOGGER.info("The source dataSet {} can't be removed because it doesn't exist ", SOURCE_DATASET_NAME);

        }
        try {
            destinationDatasetHelper.deleteDataset(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME);
        } catch (DataSetNotExistsException e) {
            LOGGER.info("The destination dataSet {} can't be removed because it doesn't exist ", DESTINATION_DATASET_NAME);

        }
        List<String> cloudIds = sourceDatasetHelper.getCloudIds();
        for (String cloudId : cloudIds) {
            adminRecordServiceClient.deleteRecord(cloudId);
            adminUisClient.deleteCloudId(cloudId);
        }

    }

    //2016-10-05 10:05:05+0200
    private String getNow() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }

}
