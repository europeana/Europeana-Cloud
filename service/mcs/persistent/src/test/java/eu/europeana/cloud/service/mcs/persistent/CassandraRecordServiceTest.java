package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.After;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

import org.mockito.Mockito;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/spiedServicesTestContext.xml"})
public class CassandraRecordServiceTest extends CassandraTestBase {

    @Autowired
    private CassandraRecordService cassandraRecordService;

    @Autowired
    private UISClientHandler uisHandler;

    @Autowired
    private SolrRepresentationIndexer representationIndexer;

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
        Mockito.reset(representationIndexer);
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

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowExpWhileCreatingRepresentationIfNoRecordInUis()
            throws Exception {

        makeUISThrowRecordNotExist();
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

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowExpWhileGettingRecordIfNoRecordInUis()
            throws Exception {
        makeUISThrowRecordNotExist();
        cassandraRecordService.getRecord("globalId");
    }

    @Test(expected = SystemException.class)
    public void shouldThrowSystemExpWhileGettingRecordIfUisFails()
            throws Exception {
        makeUISThrowSystemException();
        cassandraRecordService.getRecord("globalId");
    }

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowExpWhileDeletingRecordIfNoRecordInUis()
            throws Exception {
        makeUISThrowRecordNotExist();
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

        verify(representationIndexer, times(1)).removeRepresentationVersion(
                r1.getVersion(), dataProvider1.getPartitionKey());
        verify(representationIndexer, times(5)).insertRepresentation(
                Matchers.any(Representation.class), Matchers.anyInt());
        verifyNoMoreInteractions(representationIndexer);

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
        verify(representationIndexer, times(1)).removeRepresentation(globalId,
                represntationName, dataProvider1.getPartitionKey());
        verify(representationIndexer, times(1)).removeRepresentation(globalId,
                represntationName, dataProvider2.getPartitionKey());
        verify(representationIndexer, times(4)).insertRepresentation(
                Matchers.any(Representation.class), Matchers.anyInt());
        verifyNoMoreInteractions(representationIndexer);

        cassandraRecordService.listRepresentationVersions(globalId,
                represntationName).isEmpty();
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
        verify(representationIndexer, times(1)).removeRecordRepresentations(
                globalId, dataProvider1.getPartitionKey());
        verify(representationIndexer, times(1)).removeRecordRepresentations(
                globalId, dataProvider2.getPartitionKey());
        verify(representationIndexer, times(8)).insertRepresentation(
                Matchers.any(Representation.class), Matchers.anyInt());
        verifyNoMoreInteractions(representationIndexer);

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

    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotDeletePersistentRepresentation() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = insertDummyPersistentRepresentation("globalId",
                "dc", PROVIDER_1_ID);
        cassandraRecordService.deleteRepresentation(r.getCloudId(),
                r.getRepresentationName(), r.getVersion());
    }

    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotAddFileToPersistentRepresentation() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        Representation r = insertDummyPersistentRepresentation("globalId",
                "dc", PROVIDER_1_ID);
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null);
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
        File f = new File("content.xml", "application/xml", null, null, 0, null);
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
        File f = new File("content.xml", "application/xml", null, null, 0, null);
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
        File f = new File("content.xml", "application/xml", null, null, 0, null);
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
        // verify(representationIndexer).insertRepresentation(copy,
        // PROVIDER_1_PARTITION_KEY);
        verify(representationIndexer, times(3)).insertRepresentation(
                any(Representation.class), eq(PROVIDER_1_PARTITION_KEY));
        verifyNoMoreInteractions(representationIndexer);

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
        assertEquals(r.getRevisions().size(), 1);

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
        assertEquals(r.getRevisions().size(), 1);

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

    @Test(expected = RevisionIsNotValidException.class)
    public void addRevisionWithNullRevisionUpdatesDate() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        // given
        Representation r = cassandraRecordService.createRepresentation(
                "globalId", "edm", PROVIDER_1_ID);
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        revision.setUpdateTimeStamp(null);
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
        String revisionKey = RevisionUtils.getRevisionKey(REVISION_PROVIDER, REVISION_NAME);
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
        String revisionKey = RevisionUtils.getRevisionKey(REVISION_PROVIDER, REVISION_NAME);
        cassandraRecordService.getRevision(r.getCloudId(), r.getRepresentationName(), r.getVersion(), revisionKey);
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void getRevisionFromNonExistedRepresentation() throws Exception {
        makeUISSuccess();
        mockUISProvider1Success();
        String revisionKey = RevisionUtils.getRevisionKey(REVISION_PROVIDER, REVISION_NAME);
        cassandraRecordService.getRevision("globalId", "not_existing_schema", "5573dbf0-5979-11e6-9061-1c6f653f9042", revisionKey);
    }

    private Representation insertDummyPersistentRepresentation(String cloudId,
                                                               String schema, String providerId) throws Exception {
        Representation r = cassandraRecordService.createRepresentation(cloudId,
                schema, providerId);
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null);
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
    }

    private void makeUISFailure() throws RecordNotExistsException {
        Mockito.doReturn(false).when(uisHandler)
                .existsCloudId(Mockito.anyString());
    }

    private void makeUISThrowRecordNotExist() throws RecordNotExistsException {
        Mockito.doThrow(RecordNotExistsException.class).when(uisHandler)
                .existsCloudId(Mockito.anyString());
    }

    private void makeUISThrowSystemException() throws RecordNotExistsException {
        Mockito.doThrow(SystemException.class).when(uisHandler)
                .existsCloudId(Mockito.anyString());
    }

}
