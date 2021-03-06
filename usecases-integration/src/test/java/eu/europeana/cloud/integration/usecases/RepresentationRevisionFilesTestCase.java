package eu.europeana.cloud.integration.usecases;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.commons.io.IOUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;

import static eu.europeana.cloud.integration.usecases.IntegrationConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Tarek on 2/9/2017.
 */
public class RepresentationRevisionFilesTestCase implements TestCase {

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
        System.out.println("RepresentationRevisionFilesTestCase started ..");
        try {

            List<String> tags = new ArrayList<>(1);
            tags.add(Tags.PUBLISHED.getTag());

            String cloudId = sourceDatasetHelper.prepareCloudId(SOURCE_PROVIDER_ID);
            sourceDatasetHelper.prepareDatasetWithRecordsInside(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, SOURCE_REVISION_NAME, tags, RECORDS_NUMBERS, cloudId);
            List<Revision> revisions = getRevisions(cloudId, SOURCE_REPRESENTATION_NAME);

            assertNotNull(revisions);
            assertEquals(revisions.size(), 1);
            Revision revision = revisions.get(0);

            List<CloudTagsResponse> cloudTagsResponses = sourceDatasetHelper.getDataSetRevisions(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME, SOURCE_REPRESENTATION_NAME, revision.getRevisionName(), revision.getRevisionProviderId(), TestHelper.getUTCDateString(revision.getCreationTimeStamp()));
            assertNotNull(cloudTagsResponses);
            assertEquals(cloudTagsResponses.size(), 1);
            String responseCloudId = cloudTagsResponses.get(0).getCloudId();
            assertEquals(responseCloudId, cloudId);

            RepresentationRevisionResponse representationRevisionResponse = sourceRecordServiceClient.getRepresentationRevision(responseCloudId, SOURCE_REPRESENTATION_NAME, revision.getRevisionName(), revision.getRevisionProviderId(), TestHelper.getUTCDateString(revision.getCreationTimeStamp()));
            assertNotNull(representationRevisionResponse.getFiles());
            assertEquals(representationRevisionResponse.getFiles().size(), 1);

            InputStream stream = sourceFileServiceClient.getFile(representationRevisionResponse.getCloudId(), SOURCE_REPRESENTATION_NAME, representationRevisionResponse.getVersion(), representationRevisionResponse.getFiles().get(0).getFileName());
            List<String> lines = IOUtils.readLines(stream);
            assertNotNull(lines);
            assertEquals(lines.size(), 1);
            assertEquals(lines.get(0), FILE_CONTENT);
            //Do any Conversion to the file content; Mock it in here to use the same content
            String convertedContent = lines.get(0);

            String uri = sourceDatasetHelper.addFileToNewRepresentation(DESTINATION_REPRESENTATION_NAME, SOURCE_PROVIDER_ID, convertedContent);
            assertNotNull(uri);
            String version = sourceDatasetHelper.getVersionFromFileUri(uri);
            sourceDatasetHelper.addRevision(SOURCE_PROVIDER_ID, DESTINATION_REPRESENTATION_NAME, DESTINATION_REVISION_NAME, tags, responseCloudId, version);
            revisions = getRevisions(responseCloudId, DESTINATION_REPRESENTATION_NAME);
            assertNotNull(revisions);
            assertEquals(revisions.size(), 1);
            assertEquals(revisions.get(0).getRevisionName(), DESTINATION_REVISION_NAME);

            System.out.println("RepresentationRevisionFilesTestCase Finished Successfully ..");

        } finally {
            cleanUp();
        }
    }

    private List<Revision> getRevisions(String cloudId, String representationName) throws MCSException {
        return sourceRecordServiceClient.getRepresentation(cloudId, representationName).getRevisions();
    }


    public void cleanUp() throws CloudException, MCSException {
        System.out.println("RepresentationRevisionFilesTestCase cleaning up ..");
        Set<String> cloudIds = sourceDatasetHelper.getCloudIds();
        for (String cloudId : cloudIds) {
            adminRecordServiceClient.deleteRepresentation(cloudId, SOURCE_REPRESENTATION_NAME);
            adminRecordServiceClient.deleteRepresentation(cloudId, DESTINATION_REPRESENTATION_NAME);
            adminUisClient.deleteCloudId(cloudId);
        }
        sourceDatasetHelper.deleteDataset(SOURCE_PROVIDER_ID, SOURCE_DATASET_NAME);
        sourceDatasetHelper.cleanCloudIds();
    }
}

