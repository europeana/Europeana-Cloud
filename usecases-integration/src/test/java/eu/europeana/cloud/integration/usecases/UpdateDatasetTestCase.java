package eu.europeana.cloud.integration.usecases;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
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
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by Tarek on 9/19/2016.
 */

public class UpdateDatasetTestCase extends IntegrationConstants implements TestCase {


    @Resource
    private DatasetHelper sourceDatasetHelper;
    @Resource
    private DatasetHelper destinationDatasetHelper;
    @Resource
    private UISClient adminUisClient;

    @Resource
    private RecordServiceClient adminRecordServiceClient;

    @Resource
    private Properties appProperties;

    private final static int RECORDS_NUMBERS = 3;

    @Autowired
    private RevisionServiceClient destinationRevisionServiceClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateDatasetTestCase.class);


    public void executeTestCase() throws CloudException, MCSException, IOException {

        try {

            String now = getNow();
            prepareTestCase();
            List<CloudVersionRevisionResponse> cloudVersionRevisionResponseList = sourceDatasetHelper.getDataSetCloudIdsByRepresentation(SOURCE_DATASET_NAME, SOURCE_PROVIDER_ID, SOURCE_REPRESENTATION_NAME, now, Tags.PUBLISHED.getTag());
            assertNotNull(cloudVersionRevisionResponseList);
            assertEquals(cloudVersionRevisionResponseList.size(), RECORDS_NUMBERS);
            String newDate = getNow();
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
        } finally {
            cleanUp();
        }
    }


    private void prepareTestCase() throws MCSException, MalformedURLException, CloudException {
        List<String> tags = new ArrayList<>();
        tags.add(Tags.PUBLISHED.getTag());
        tags.add(Tags.DELETED.getTag());
        sourceDatasetHelper.prepareDatasetWithRecordsInside(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, SOURCE_REVISION_NAME, tags, RECORDS_NUMBERS);
        destinationDatasetHelper.prepareEmptyDataset(DESTINATION_PROVIDER_ID, DESTINATION_DATASET_NAME);

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
