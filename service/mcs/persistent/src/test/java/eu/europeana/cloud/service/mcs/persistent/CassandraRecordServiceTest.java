package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

import static eu.europeana.cloud.service.mcs.Storage.DATA_BASE;
import static eu.europeana.cloud.service.mcs.Storage.OBJECT_STORAGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/spiedServicesTestContext.xml"})
public class CassandraRecordServiceTest extends CassandraTestBase {

    private static final UUID VERSION = UUID.fromString(new com.eaio.uuid.UUID().toString());

    @Autowired
    private CassandraRecordService cassandraRecordService;

    @Autowired
    private CassandraDataSetService cassandraDataSetService;

    @Autowired
    private UISClientHandler uisHandler;

    private static final String PROVIDER_1_ID = "provider1";
    private static final int PROVIDER_1_PARTITION_KEY = 0;
    private static final String PROVIDER_2_ID = "provider2";
    private static final int PROVIDER_2_PARTITION_KEY = 1;

    private static final String REVISION_PROVIDER = "revisionProvider";
    private static final String REVISION_NAME = "revisionName";

    private DataProvider dataProvider1;
    private DataProvider dataProvider2;

    @After
    public void cleanUp() {
        Mockito.reset(uisHandler);
    }

    @Test
    public void shouldCreateAndGetRepresentation() throws Exception {
        mockUISProvider1Success();
        makeUISSuccess();

        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID);

        Representation rFetched = cassandraRecordService.getRepresentation(
                "globalId", "dc", r.getVersion());
        assertThat(rFetched, is(r));
    }

    @Test
    public void shouldCreateRepresentationInGivenVersion() throws Exception {
        mockUISProvider1Success();
        makeUISSuccess();

        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID, VERSION);

        Representation rFetched = cassandraRecordService.getRepresentation(
                "globalId", "dc", VERSION.toString());
        assertThat(rFetched, is(r));
    }

    @Test
    public void shouldAllowInvokeCreateRepresentationInTheSameGivenVersionManyTimes() throws Exception {
        mockUISProvider1Success();
        makeUISSuccess();

        Representation r1 = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID, VERSION);
        Representation r2 = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID, VERSION);

        Representation rFetched = cassandraRecordService.getRepresentation(
                "globalId", "dc", VERSION.toString());
        assertThat(rFetched, is(r1));
        assertThat(rFetched, is(r2));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExpWhileCreatingRepresentationIfNoRecordInUis()
            throws Exception {

        makeUISThrowIllegalStateException();
        mockUISProvider1Success();
        cassandraRecordService.createRepresentation("globalId", "dc",
                PROVIDER_1_ID);
    }

    @Test(expected = SystemException.class)
    public void shouldThrowSystemExpWhileCreatingRepresentationIfUisFails()
            throws Exception {
        mockUISProvider1Success();
        makeUISThrowSystemException();

        cassandraRecordService.createRepresentation("globalId", "dc",
                PROVIDER_1_ID);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExpWhileGettingRecordIfNoRecordInUis()
            throws Exception {
        makeUISThrowIllegalStateException();
        cassandraRecordService.getRecord("globalId");
    }

    @Test(expected = SystemException.class)
    public void shouldThrowSystemExpWhileGettingRecordIfUisFails()
            throws Exception {
        makeUISThrowSystemException();
        cassandraRecordService.getRecord("globalId");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExpWhileDeletingRecordIfNoRecordInUis()
            throws Exception {
        makeUISThrowIllegalStateException();
        cassandraRecordService.deleteRecord("globalId");
    }

    @Test(expected = SystemException.class)
    public void shouldThrowSystemExpWhileDeletingRecordIfUisFails()
            throws Exception {
        makeUISThrowSystemException();
        cassandraRecordService.deleteRecord("globalId");
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldNotGetRepresentationIfNoPersistentExists()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        cassandraRecordService.createRepresentation("globalId", "dc",
                PROVIDER_1_ID);
        cassandraRecordService.getRepresentation("globalId", "dc");
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotFoundExpWhenNoSuchRepresentation()
            throws Exception {
        makeUISSuccess();
        cassandraRecordService.getRepresentation("globalId",
                "not_existing_schema");
    }

    @Test
    public void shouldGetLatestPersistentRepresentation() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        cassandraRecordService.createRepresentation("globalId", "dc",
                PROVIDER_1_ID);
        insertDummyPersistentRepresentation("globalId", "dc", PROVIDER_1_ID);
        cassandraRecordService.createRepresentation("globalId", "dc",
                PROVIDER_1_ID);
        Representation r4 = insertDummyPersistentRepresentation("globalId",
                "dc", PROVIDER_1_ID);
        cassandraRecordService.createRepresentation("globalId", "dc",
                PROVIDER_1_ID);

        Representation rFetched = cassandraRecordService.getRepresentation(
                "globalId", "dc");
        assertThat(rFetched, is(r4));
    }

    @Test(expected = ProviderNotExistsException.class)
    public void shouldNotCreateRepresentationForNotExistingProvider()
            throws Exception {
        makeUISFailure();
        makeUISProviderFailure();
        cassandraRecordService.createRepresentation("globalId", "dc",
                "not-existing");
    }

    @Test
    public void shouldListAllRepresentationVersionsInOrder() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r1 = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID);
        Representation r2 = insertDummyPersistentRepresentation("globalId",
                "dc", PROVIDER_1_ID);
        Representation r3 = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID);
        Representation r4 = insertDummyPersistentRepresentation("globalId",
                "dc", PROVIDER_1_ID);
        Representation r5 = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID);
        cassandraRecordService.createRepresentation("globalId", "jpg",
                PROVIDER_1_ID);

        List<Representation> representationVersions = cassandraRecordService
                .listRepresentationVersions("globalId", "dc");
        assertThat(representationVersions,
                is(Arrays.asList(r5, r4, r3, r2, r1)));
    }

    @Test
    public void shouldReturnWholeRecord()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // only temp representation
        cassandraRecordService.createRepresentation("globalId", "dc", PROVIDER_1_ID);

        // only persistent representation
        Representation jpg = insertDummyPersistentRepresentation("globalId", "jpg", PROVIDER_1_ID);

        // mixture of persistent and temp representations
        cassandraRecordService.createRepresentation("globalId", "edm", PROVIDER_1_ID);
        insertDummyPersistentRepresentation("globalId", "edm", PROVIDER_1_ID);
        cassandraRecordService.createRepresentation("globalId", "edm", PROVIDER_1_ID);
        Representation edm4 = insertDummyPersistentRepresentation("globalId", "edm", PROVIDER_1_ID);
        cassandraRecordService.createRepresentation("globalId", "edm", PROVIDER_1_ID);

        Record record = cassandraRecordService.getRecord("globalId");
        Set<Representation> expectedRepresentations = new HashSet<>(Arrays.asList(jpg, edm4));
        Set<Representation> fetchedRepresentations = new HashSet<>(record.getRepresentations());
        assertThat(fetchedRepresentations, is(expectedRepresentations));
    }

    @Test
    public void shouldDeleteRepresentationInSpecifiedVersion() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        mockUISProvider2Success();
        // given
        Representation r1 = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID);
        Representation r2 = insertDummyPersistentRepresentation("globalId",
                "dc", PROVIDER_1_ID);
        Representation r3 = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID);
        Representation r4 = cassandraRecordService.createRepresentation(
                "globalId", "dc", PROVIDER_1_ID);

        // when
        cassandraRecordService.deleteRepresentation(r1.getCloudId(),
                r1.getRepresentationName(), r1.getVersion());

        List<Representation> representationVersions = cassandraRecordService
                .listRepresentationVersions("globalId", "dc");
        assertThat(representationVersions, is(Arrays.asList(r4, r3, r2)));
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldDeleteAllRepresentationVersions() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        mockUISProvider2Success();
        // given
        final String globalId = "globalId";
        final String represntationName = "dc";
        cassandraRecordService.createRepresentation(globalId,
                represntationName, PROVIDER_1_ID);
        insertDummyPersistentRepresentation(globalId, represntationName,
                PROVIDER_2_ID);
        cassandraRecordService.createRepresentation(globalId,
                represntationName, PROVIDER_1_ID);

        // when
        cassandraRecordService
                .deleteRepresentation(globalId, represntationName);

        // then
        cassandraRecordService.listRepresentationVersions(globalId,
                represntationName).isEmpty();
    }


    @Test(expected = RepresentationNotExistsException.class)
    public void shouldDeleteAllRepresentationVersionsWhenDeletingRecord() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        mockUISProvider2Success();
        // given
        final String globalId = "globalId";
        final String representationName = "dc";
        cassandraRecordService.createRepresentation(globalId,
                representationName, PROVIDER_1_ID);
        insertDummyPersistentRepresentation(globalId, representationName,
                PROVIDER_2_ID);
        cassandraRecordService.createRepresentation(globalId,
                representationName, PROVIDER_1_ID);

        // when
        cassandraRecordService
                .deleteRecord(globalId);

        cassandraRecordService.listRepresentationVersions(globalId,
                representationName).isEmpty();
    }

    @Test
    public void shouldDeleteRepresentationRevisionObjectsWhenRecordIsDeleted()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        String cloudId = "cloud-2";

        // create new representation
        Representation r = cassandraRecordService.createRepresentation(cloudId,
                "representation-1", PROVIDER_1_ID);

        // create and add new revision
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);

        // add files to representation version
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent(cloudId, "representation-1", r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));
        // retrieve representation again
        r = cassandraRecordService.getRepresentation(cloudId, "representation-1", r.getVersion());
        // insert info to extra table
        cassandraRecordService.insertRepresentationRevision(cloudId, "representation-1", REVISION_PROVIDER, REVISION_NAME, r.getVersion(), revision.getCreationTimeStamp());

        // retrieve info from extra table
        List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions(cloudId, "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());
        assertThat(representationRevisions.size(),is(1));
        assertThat(representationRevisions.get(0).getCloudId(), is(r.getCloudId()));
        assertThat(representationRevisions.get(0).getRepresentationName(), is(r.getRepresentationName()));
        assertThat(RevisionUtils.getRevisionKey(representationRevisions.get(0).getRevisionProviderId(),
                representationRevisions.get(0).getRevisionName(),
                representationRevisions.get(0).getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
        assertThat(representationRevisions.get(0).getFiles(), is(r.getFiles()));

        cassandraRecordService.deleteRecord(cloudId);

        List<RepresentationRevisionResponse> response = cassandraRecordService.getRepresentationRevisions(cloudId, "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());
        assertEquals(0, response.size());
    }


    @Test
    public void shouldDeleteRepresentationRevisionsFromDataSetsRevisionsTablesWhenRecordIsDeleted()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        String cloudId = "cloud-2";
        String representationName = "representation-1";

        // create new representation
        Representation r = cassandraRecordService.createRepresentation(cloudId,
                "representation-1", PROVIDER_1_ID);

        // create and add new revision
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        revision.setPublished(true);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);

        // add files to representation version
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent(cloudId, representationName, r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));


        // given particular data set and representations in it
        String dsName = "ds";
        DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_1_ID, dsName,
                "description of this set");
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
                r.getCloudId(), r.getRepresentationName(), r.getVersion());

        ResultSlice<CloudTagsResponse> responseResultSlice = cassandraDataSetService.getDataSetsRevisions(ds.getProviderId(), ds.getId(), revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp(), representationName, null, 100);
        assertNotNull(responseResultSlice.getResults());
        assertEquals(1, responseResultSlice.getResults().size());

        cassandraRecordService.deleteRecord(cloudId);

        responseResultSlice = cassandraDataSetService.getDataSetsRevisions(ds.getProviderId(), ds.getId(), revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp(), representationName, null, 100);
        assertNotNull(responseResultSlice.getResults());
        assertEquals(0, responseResultSlice.getResults().size());
    }


    @Test()
    public void shouldDeleteAllRecord() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        mockUISProvider2Success();
        // given
        final String globalId = "globalId";
        final String represntationName1 = "edm";
        final String represntationName2 = "dc";
        cassandraRecordService.createRepresentation(globalId,
                represntationName2, PROVIDER_1_ID);
        insertDummyPersistentRepresentation(globalId, "jpg", PROVIDER_1_ID);
        cassandraRecordService.createRepresentation(globalId,
                represntationName1, PROVIDER_1_ID);
        insertDummyPersistentRepresentation(globalId, represntationName1,
                PROVIDER_1_ID);
        insertDummyPersistentRepresentation(globalId, represntationName1,
                PROVIDER_2_ID);

        // when
        cassandraRecordService.deleteRecord(globalId);

        // then
        try {
            cassandraRecordService.listRepresentationVersions(globalId,
                    represntationName1);
            fail("Expected to be thrown RepresentationNotExistsException");
        } catch (RepresentationNotExistsException e) {/* do nothing */

        }

        try {
            cassandraRecordService.listRepresentationVersions(globalId,
                    represntationName1);
            fail("Expected to be thrown RepresentationNotExistsException");
        } catch (RepresentationNotExistsException e) { /* do nothing */

        }
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowExcWhenDeletingRecordHasNoRepresentations()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        // record does not have any representation

        // when
        cassandraRecordService.deleteRecord("globalId");
        // then should throw RepresentationNotExistsException
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowExcWhenDeletingRecordForTheSecondTime()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        cassandraRecordService.createRepresentation("globalId", "dc",
                PROVIDER_1_ID);
        // delete record
        cassandraRecordService.deleteRecord("globalId");
        // when deleting for the second time
        cassandraRecordService.deleteRecord("globalId");

    }

    @Test
    public void shouldDeletePersistentRepresentation() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = insertDummyPersistentRepresentation("globalId", "dc", PROVIDER_1_ID);
        cassandraRecordService.deleteRepresentation(r.getCloudId(), r.getRepresentationName(), r.getVersion());

        assertTrue(true);
    }

    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotAddFileToPersistentRepresentation() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = insertDummyPersistentRepresentation("globalId",
                "dc", PROVIDER_1_ID);
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null, OBJECT_STORAGE);
        cassandraRecordService.putContent(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));
    }

    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotRemoveFileFromPersistentRepresentation()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = insertDummyPersistentRepresentation("globalId",
                "dc", PROVIDER_1_ID);

        File f = r.getFiles().get(0);
        cassandraRecordService.deleteContent(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), f.getFileName());
    }

    @Test(expected = CannotPersistEmptyRepresentationException.class)
    public void shouldNotPersistRepresentationWithoutFile() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        cassandraRecordService.persistRepresentation(r.getCloudId(),
                r.getRepresentationName(), r.getVersion());
    }

    @Test
    public void shouldPutAndGetFile() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);

        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null, OBJECT_STORAGE);
        cassandraRecordService.putContent(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));

        r = cassandraRecordService.getRepresentation(r.getCloudId(),
                r.getRepresentationName(), r.getVersion());
        assertThat(r.getFiles().size(), is(1));
        File fetchedFile = r.getFiles().get(0);
        assertThat(fetchedFile.getFileName(), is(f.getFileName()));
        assertThat(fetchedFile.getMimeType(), is(f.getMimeType()));
        assertThat(fetchedFile.getContentLength(),
                is((long) dummyContent.length));
        String contentMd5 = Hashing.md5().hashBytes(dummyContent).toString();
        assertThat(fetchedFile.getMd5(), is(contentMd5));
    }

    @Test
    public void shouldPutAndGetFileStoredInDb() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);

        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null, DATA_BASE);
        cassandraRecordService.putContent(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));

        r = cassandraRecordService.getRepresentation(r.getCloudId(),
                r.getRepresentationName(), r.getVersion());
        assertThat(r.getFiles().size(), is(1));
        File fetchedFile = r.getFiles().get(0);
        assertThat(fetchedFile.getFileName(), is(f.getFileName()));
        assertThat(fetchedFile.getMimeType(), is(f.getMimeType()));
        assertThat(fetchedFile.getContentLength(),
                is((long) dummyContent.length));
        String contentMd5 = Hashing.md5().hashBytes(dummyContent).toString();
        assertThat(fetchedFile.getMd5(), is(contentMd5));
    }

    @Test
    public void shouldGetContent() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);

        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null, OBJECT_STORAGE);
        cassandraRecordService.putContent(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));
        ByteArrayOutputStream baos = new ByteArrayOutputStream(
                dummyContent.length);
        cassandraRecordService.getContent(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), f.getFileName(),
                baos);
        assertThat(baos.toByteArray(), is(dummyContent));
    }

    @Test
    public void shouldRemoveFile() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);

        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null, OBJECT_STORAGE);
        cassandraRecordService.putContent(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));

        // when
        cassandraRecordService.deleteContent(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), f.getFileName());

        // then
        r = cassandraRecordService.getRepresentation(r.getCloudId(),
                r.getRepresentationName(), r.getVersion());
        assertTrue(r.getFiles().isEmpty());
    }

    // @Test TODO
    public void shouldCopyRepresentation() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = insertDummyPersistentRepresentation("globalId",
                "dc", PROVIDER_1_ID);
        // when
        Representation copy = cassandraRecordService.copyRepresentation(
                r.getCloudId(), r.getRepresentationName(), r.getVersion());
        // that
        assertThat(copy.getCloudId(), is(r.getCloudId()));
        assertThat(copy.getDataProvider(), is(r.getDataProvider()));
        assertThat(copy.getRepresentationName(), is(r.getRepresentationName()));
        for (File f : r.getFiles()) {
            ByteArrayOutputStream rContent = new ByteArrayOutputStream();
            ByteArrayOutputStream copyContent = new ByteArrayOutputStream();
            cassandraRecordService.getContent(r.getCloudId(),
                    r.getRepresentationName(), r.getVersion(), f.getFileName(),
                    rContent);
            cassandraRecordService.getContent(copy.getCloudId(),
                    copy.getRepresentationName(), copy.getVersion(),
                    f.getFileName(), copyContent);
            assertThat(rContent.toByteArray(), is(copyContent.toByteArray()));
        }
    }

    @Test
    public void addRevision() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);
        // then
        r = cassandraRecordService.getRepresentation(r.getCloudId(),
                r.getRepresentationName(), r.getVersion());
        assertNotNull(r.getRevisions());
        assertFalse(r.getRevisions().isEmpty());
        assertEquals(1, r.getRevisions().size());

    }

    @Test
    public void addAlreadyExistedRevision() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);
        // then
        r = cassandraRecordService.getRepresentation(r.getCloudId(),
                r.getRepresentationName(), r.getVersion());
        assertNotNull(r.getRevisions());
        assertFalse(r.getRevisions().isEmpty());
        assertEquals(1, r.getRevisions().size());

    }

    @Test(expected = RevisionIsNotValidException.class)
    public void addRevisionWithNullRevision() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), null);
    }

    @Test(expected = RevisionIsNotValidException.class)
    public void addRevisionWithNullRevisionName() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        Revision revision = new Revision(null, REVISION_PROVIDER);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);
    }

    @Test(expected = RevisionIsNotValidException.class)
    public void addRevisionWithNullRevisionProvider() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        Revision revision = new Revision(REVISION_NAME, null);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);
    }

    @Test(expected = RevisionIsNotValidException.class)
    public void addRevisionWithNullRevisionCreationDate() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        revision.setCreationTimeStamp(null);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);
    }

    @Test
    public void getRevision() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER, new Date(), true, false, true);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);
        // then
        String revisionKey = RevisionUtils.getRevisionKey(revision);
        Revision storedRevision = cassandraRecordService.getRevision(r.getCloudId(), r.getRepresentationName(), r.getVersion(), revisionKey);
        assertNotNull(storedRevision);
        assertThat(storedRevision, is(revision));

    }


    @Test(expected = RevisionNotExistsException.class)
    public void getNonExistedRevision() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        // then
        String revisionKey = RevisionUtils.getRevisionKey(REVISION_PROVIDER, REVISION_NAME, new Date().getTime());
        cassandraRecordService.getRevision(r.getCloudId(), r.getRepresentationName(), r.getVersion(), revisionKey);
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void getRevisionFromNonExistedRepresentation() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        String revisionKey = RevisionUtils.getRevisionKey(REVISION_PROVIDER, REVISION_NAME, new Date().getTime());
        cassandraRecordService.getRevision("globalId", "not_existing_schema", "5573dbf0-5979-11e6-9061-1c6f653f9042", revisionKey);
    }

    private Representation insertDummyPersistentRepresentation(String cloudId,
                                                               String schema, String providerId) throws Exception {
        Representation r = cassandraRecordService.createRepresentation(cloudId,
                schema, providerId);
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null, OBJECT_STORAGE);
        cassandraRecordService.putContent(cloudId, schema, r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));

        return cassandraRecordService.persistRepresentation(r.getCloudId(),
                r.getRepresentationName(), r.getVersion());
    }

    private void mockUISProvider1Success() {
        dataProvider1 = new DataProvider();
        dataProvider1.setId(PROVIDER_1_ID);
        dataProvider1.setPartitionKey(PROVIDER_1_PARTITION_KEY);

        Mockito.doReturn(dataProvider1).when(uisHandler)
                .getProvider(PROVIDER_1_ID);
    }

    private void mockUISProvider2Success() {
        dataProvider2 = new DataProvider();
        dataProvider2.setId(PROVIDER_2_ID);
        dataProvider2.setPartitionKey(PROVIDER_2_PARTITION_KEY);

        Mockito.doReturn(dataProvider2).when(uisHandler)
                .getProvider(PROVIDER_2_ID);
    }

    private void makeUISProviderFailure() {
        Mockito.doReturn(null).when(uisHandler)
                .getProvider(Mockito.anyString());
    }

    private void makeUISSuccess() throws RecordNotExistsException {
        Mockito.doReturn(true).when(uisHandler)
                .existsCloudId(Mockito.anyString());
        Mockito.when(uisHandler.existsProvider(Mockito.anyString())).thenReturn(true);
    }

    private void makeUISFailure() throws RecordNotExistsException {
        Mockito.doReturn(false).when(uisHandler)
                .existsCloudId(Mockito.anyString());
    }

    private void makeUISThrowIllegalStateException()  {
        Mockito.doThrow(IllegalStateException.class).when(uisHandler)
                .existsCloudId(Mockito.anyString());
    }

    private void makeUISThrowSystemException() {
        Mockito.doThrow(SystemException.class).when(uisHandler)
                .existsCloudId(Mockito.anyString());
    }


    @Test
    public void shouldReturnRepresentationRevisionObjectRevisionLatest()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();

        // create new representation
        Representation r = cassandraRecordService.createRepresentation("cloud-1",
                "representation-1", PROVIDER_1_ID);

        // create and add new revision
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);
        Revision revisionLatest = new Revision(REVISION_NAME, REVISION_PROVIDER);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revisionLatest);

        // insert info to extra table
        cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", revision.getRevisionProviderId(), revision.getRevisionName(), r.getVersion(), revision.getCreationTimeStamp());
        cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", revisionLatest.getRevisionProviderId(), revisionLatest.getRevisionName(), r.getVersion(), revisionLatest.getCreationTimeStamp());

        // retrieve info from extra table
        List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME, null);

        assertThat(representationRevisions.size(), is(1));
        assertThat(representationRevisions.get(0).getCloudId(), is(r.getCloudId()));
        assertThat(representationRevisions.get(0).getRepresentationName(), is(r.getRepresentationName()));
        assertThat(RevisionUtils.getRevisionKey(representationRevisions.get(0).getRevisionProviderId(),
                representationRevisions.get(0).getRevisionName(),
                representationRevisions.get(0).getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revisionLatest)));
        assertThat(representationRevisions.get(0).getRevisionTimestamp(), is(revisionLatest.getCreationTimeStamp()));

        // get the other revision
        representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());

        assertThat(representationRevisions.size(), is(1));
        assertThat(representationRevisions.get(0).getCloudId(), is(r.getCloudId()));
        assertThat(representationRevisions.get(0).getRepresentationName(), is(r.getRepresentationName()));
        assertThat(RevisionUtils.getRevisionKey(representationRevisions.get(0).getRevisionProviderId(),
                representationRevisions.get(0).getRevisionName(),
                representationRevisions.get(0).getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
        assertThat(representationRevisions.get(0).getRevisionTimestamp(), is(revision.getCreationTimeStamp()));
    }

    @Test
    public void shouldReturnRepresentationRevisionObjectRevisionFirst()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();

        // create new representation
        Representation r = cassandraRecordService.createRepresentation("cloud-1",
                "representation-1", PROVIDER_1_ID);

        // create and add new revision
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);

        // insert info to extra table
        cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", revision.getRevisionProviderId(), revision.getRevisionName(), r.getVersion(), revision.getCreationTimeStamp());

        // retrieve info from extra table
        List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());

        assertThat(representationRevisions.size(), is(1));
        assertThat(representationRevisions.get(0).getCloudId(), is(r.getCloudId()));
        assertThat(representationRevisions.get(0).getRepresentationName(), is(r.getRepresentationName()));
        assertThat(RevisionUtils.getRevisionKey(representationRevisions.get(0).getRevisionProviderId(),
                representationRevisions.get(0).getRevisionName(),
                representationRevisions.get(0).getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
        assertThat(representationRevisions.get(0).getFiles(), is(r.getFiles()));
        assertThat(representationRevisions.get(0).getFiles().size(), is(0));

        // add files to representation version
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent("cloud-1", "representation-1", r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));

        // retrieve representation again
        r = cassandraRecordService.getRepresentation("cloud-1", "representation-1", r.getVersion());

        // retrieve info from extra table again
        representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());
        assertThat(representationRevisions.size(), is(1));
        assertThat(representationRevisions.get(0).getCloudId(), is(r.getCloudId()));
        assertThat(representationRevisions.get(0).getRepresentationName(), is(r.getRepresentationName()));
        assertThat(RevisionUtils.getRevisionKey(representationRevisions.get(0).getRevisionProviderId(),
                representationRevisions.get(0).getRevisionName(),
                representationRevisions.get(0).getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
        assertThat(representationRevisions.get(0).getFiles(), is(r.getFiles()));
    }


    @Test
    public void shouldDeleteRepresentationRevisionObjectWhenRepresentationIsDeleted()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();

        // create new representation
        Representation r = cassandraRecordService.createRepresentation("cloud-1",
                "representation-1", PROVIDER_1_ID);

        // create and add new revision
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);

        // add files to representation version
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent("cloud-1", "representation-1", r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));
        // retrieve representation again
        r = cassandraRecordService.getRepresentation("cloud-1", "representation-1", r.getVersion());

        // insert info to extra table
        cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME, r.getVersion(), revision.getCreationTimeStamp());

        // retrieve info from extra table
        List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());

        assertThat(representationRevisions.size(), is(1));
        assertThat(representationRevisions.get(0).getCloudId(), is(r.getCloudId()));
        assertThat(representationRevisions.get(0).getRepresentationName(), is(r.getRepresentationName()));
        assertThat(RevisionUtils.getRevisionKey(representationRevisions.get(0).getRevisionProviderId(),
                representationRevisions.get(0).getRevisionName(),
                representationRevisions.get(0).getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
        assertThat(representationRevisions.get(0).getFiles(), is(r.getFiles()));

        cassandraRecordService.deleteRepresentation("cloud-1", "representation-1");

        // retrieve info from extra table again
        List<RepresentationRevisionResponse> response = cassandraRecordService.getRepresentationRevisions("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());
        assertEquals(0, response.size());
    }


    @Test
    public void shouldReturnRepresentationRevisionObjectFilesFirst()
            throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = insertDummyPersistentRepresentation("cloud-1", "representation-1", PROVIDER_1_ID);
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);
        cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME, r.getVersion(), revision.getCreationTimeStamp());
        List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());

        assertThat(representationRevisions.get(0).getCloudId(), is(r.getCloudId()));
        assertThat(representationRevisions.get(0).getRepresentationName(), is(r.getRepresentationName()));
        assertThat(RevisionUtils.getRevisionKey(representationRevisions.get(0).getRevisionProviderId(),
                representationRevisions.get(0).getRevisionName(),
                representationRevisions.get(0).getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
        assertThat(representationRevisions.get(0).getFiles(), is(r.getFiles()));
    }
}
