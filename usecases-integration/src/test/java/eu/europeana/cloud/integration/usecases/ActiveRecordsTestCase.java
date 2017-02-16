package eu.europeana.cloud.integration.usecases;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.commons.io.IOUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static eu.europeana.cloud.integration.usecases.IntegrationConstants.*;

/**
 * Created by Tarek on 2/9/2017.
 */
public class ActiveRecordsTestCase implements TestCase {

    @Resource
    private DatasetHelper sourceDatasetHelper;

    @Resource
    private UISClient adminUisClient;

    @Resource
    private RecordServiceClient adminRecordServiceClient;

    @Resource
    private RecordServiceClient sourceRecordServiceClient;

    @Resource
    private FileServiceClient sourceFileServiceClient;

    private final static int RECORDS_NUMBERS = 3;


    public void executeTestCase() throws CloudException, MCSException, IOException {
        System.out.println("ActiveRecordsTestCase started ..");
        try {
            List<CloudIdAndTimestampResponse> dereferenceCloudIdAndTimestampResponseList = getLatestCloudIdAndTimestampResponsesForDereferenceRevision();

            assertNotNull(dereferenceCloudIdAndTimestampResponseList);
            assertEquals(dereferenceCloudIdAndTimestampResponseList.size(), RECORDS_NUMBERS);

            List<CloudIdAndTimestampResponse> publishedCloudIdAndTimestampResponseList = getLatestCloudIdAndTimestampResponsesForPublishRevision();
            assertNotNull(publishedCloudIdAndTimestampResponseList);
            assertEquals(publishedCloudIdAndTimestampResponseList.size(), 1);

            List<CloudIdAndTimestampResponse> intersectedCloudIdAndTimestamps = intersectCloudIdAndTimestampResponsesBasedOnCloudId(dereferenceCloudIdAndTimestampResponseList, publishedCloudIdAndTimestampResponseList);
            assertNotNull(intersectedCloudIdAndTimestamps);
            assertEquals(intersectedCloudIdAndTimestamps.size(), 1);

            String utcFormateDateString = getUTCDateString(intersectedCloudIdAndTimestamps);
            RepresentationRevisionResponse representationRevisionResponse = sourceRecordServiceClient.getRepresentationRevision(intersectedCloudIdAndTimestamps.get(0).getCloudId(), SOURCE_REPRESENTATION_NAME, DEREFERENCE_REVISION, SOURCE_PROVIDER_ID, utcFormateDateString);
            assertNotNull(representationRevisionResponse.getFiles());
            assertEquals(representationRevisionResponse.getFiles().size(), 1);

            InputStream stream = sourceFileServiceClient.getFile(representationRevisionResponse.getCloudId(), SOURCE_REPRESENTATION_NAME, representationRevisionResponse.getVersion(), representationRevisionResponse.getFiles().get(0).getFileName());
            List<String> lines = IOUtils.readLines(stream);
            assertNotNull(lines);
            assertEquals(lines.size(), 1);
            assertEquals(lines.get(0), FILE_CONTENT);

            //Do any Conversion to the file content; Mock it in here to use the same content
            String convertedContent = lines.get(0);
            String uri = sourceDatasetHelper.addFileToNewRepresentation(SOURCE_REPRESENTATION_NAME, SOURCE_PROVIDER_ID, convertedContent);
            assertNotNull(uri);

            System.out.println("ActiveRecordsTestCase Finished Successfully ..");

        } finally {
            cleanUp();
        }
    }

    private List<CloudIdAndTimestampResponse> getLatestCloudIdAndTimestampResponsesForPublishRevision() throws MCSException, MalformedURLException, CloudException {
        List<String> tags;
        tags = new ArrayList<>();
        tags.add(Tags.PUBLISHED.getTag());
        Set<String> cloudIds = sourceDatasetHelper.getCloudIds();
        //prepare test case for one shared cloudId
        prepareTestCase(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, PUBLISH_REVISION, tags, RECORDS_NUMBERS, cloudIds.iterator().next());

        return sourceDatasetHelper.getLatestDataSetCloudIdByRepresentationAndRevision(SOURCE_DATASET_NAME, SOURCE_PROVIDER_ID, SOURCE_PROVIDER_ID, PUBLISH_REVISION, SOURCE_REPRESENTATION_NAME, false);
    }

    private List<CloudIdAndTimestampResponse> getLatestCloudIdAndTimestampResponsesForDereferenceRevision() throws MCSException, MalformedURLException, CloudException {
        List<String> tags = new ArrayList<>();
        tags.add(Tags.PUBLISHED.getTag());
        tags.add(Tags.DELETED.getTag());

        prepareTestCase(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, DEREFERENCE_REVISION, tags, RECORDS_NUMBERS, null);
        return sourceDatasetHelper.getLatestDataSetCloudIdByRepresentationAndRevision(SOURCE_DATASET_NAME, SOURCE_PROVIDER_ID, SOURCE_PROVIDER_ID, DEREFERENCE_REVISION, SOURCE_REPRESENTATION_NAME, null);
    }

    private String getUTCDateString(List<CloudIdAndTimestampResponse> intersectedCloudIdAndTimestamps) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(intersectedCloudIdAndTimestamps.get(0).getRevisionTimestamp());
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
        System.out.println("ActiveRecordsTestCase cleaning up ..");
        Set<String> cloudIds = sourceDatasetHelper.getCloudIds();
        for (String cloudId : cloudIds) {
            adminRecordServiceClient.deleteRepresentation(cloudId, SOURCE_REPRESENTATION_NAME);
            adminUisClient.deleteCloudId(cloudId);
        }
        sourceDatasetHelper.deleteDataset(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME);
        sourceDatasetHelper.cleanCloudIds();
    }
}

