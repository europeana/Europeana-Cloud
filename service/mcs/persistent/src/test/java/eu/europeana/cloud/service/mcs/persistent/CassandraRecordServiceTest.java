package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.service.mcs.UISClientHandler;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.hash.Hashing;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import org.junit.After;

/**
 * 
 * @author sielski
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/spiedServicesTestContext.xml" })
public class CassandraRecordServiceTest extends CassandraTestBase {

    @Autowired
    private CassandraRecordService cassandraRecordService;

    @Autowired
    private UISClientHandler uisHandler;

    private static final String providerId = "provider";


    @After
    public void cleanUp() {
        Mockito.reset(uisHandler);
    }


    @Test
    public void shouldCreateAndGetRepresentation()
            throws Exception {
        makeUISProviderSuccess();
        makeUISSuccess();

        Representation r = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

        Representation rFetched = cassandraRecordService.getRepresentation("globalId", "dc", r.getVersion());
        assertThat(rFetched, is(r));
    }


    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowExpWhileCreatingRepresentationIfNoRecordInUis()
            throws Exception {

        makeUISThrowRecordNotExist();
        makeUISProviderSuccess();
        Representation r = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
    }


    @Test(expected = SystemException.class)
    public void shouldThrowSystemExpWhileCreatingRepresentationIfUisFails()
            throws Exception {
        makeUISProviderSuccess();
        makeUISThrowSystemException();

        Representation r = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
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
        Record record = cassandraRecordService.getRecord("globalId");
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
        makeUISProviderSuccess();
        Representation r = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
        Representation rFetched = cassandraRecordService.getRepresentation("globalId", "dc");
    }


    @Test
    public void shouldGetLatestPersistentRepresentation()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r1 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
        Representation r2 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
        Representation r3 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
        Representation r4 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
        Representation r5 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

        Representation rFetched = cassandraRecordService.getRepresentation("globalId", "dc");
        assertThat(rFetched, is(r4));
    }


    @Test(expected = ProviderNotExistsException.class)
    public void shouldNotCreateRepresentationForNotExistingProvider()
            throws Exception {
        makeUISFailure();
        makeUISProviderFailure();
        cassandraRecordService.createRepresentation("globalId", "dc", "not-existing");
    }


    @Test
    public void shouldListAllRepresentationVersionsInOrder()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r1 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
        Representation r2 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
        Representation r3 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
        Representation r4 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
        Representation r5 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
        Representation rep_another_schema = cassandraRecordService.createRepresentation("globalId", "jpg", providerId);
        r2.setFiles(Collections.EMPTY_LIST);
        r4.setFiles(Collections.EMPTY_LIST);

        List<Representation> representationVersions = cassandraRecordService.listRepresentationVersions("globalId",
            "dc");
        assertThat(representationVersions, is(Arrays.asList(r5, r4, r3, r2, r1)));
    }


    @Test
    public void shouldReturnWholeRecord()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        // only temp representation
        Representation dc = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

        // only persistent representation
        Representation jpg = insertDummyPersistentRepresentation("globalId", "jpg", providerId);

        // mixture of persistent and temp representations
        Representation edm1 = cassandraRecordService.createRepresentation("globalId", "edm", providerId);
        Representation edm2 = insertDummyPersistentRepresentation("globalId", "edm", providerId);
        Representation edm3 = cassandraRecordService.createRepresentation("globalId", "edm", providerId);
        Representation edm4 = insertDummyPersistentRepresentation("globalId", "edm", providerId);
        Representation edm5 = cassandraRecordService.createRepresentation("globalId", "edm", providerId);

        // we will get representations with empty file lists - for comparison,
        // we should remove files from our representations
        jpg.setFiles(Collections.EMPTY_LIST);
        edm4.setFiles(Collections.EMPTY_LIST);

        Record record = cassandraRecordService.getRecord("globalId");
        Set<Representation> expectedRepresentations = new HashSet<>(Arrays.asList(jpg, edm4));
        Set<Representation> fetchedRepresentations = new HashSet<>(record.getRepresentations());
        assertThat(fetchedRepresentations, is(expectedRepresentations));
    }


    @Test
    public void shouldDeleteRepresentationInSpecifiedVersion()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r1 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
        Representation r2 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
        Representation r3 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

        cassandraRecordService.deleteRepresentation(r1.getRecordId(), r1.getSchema(), r1.getVersion());

        // we will get representations with empty file lists - for comparison,
        // we should remove files from our representations
        r2.setFiles(Collections.EMPTY_LIST);

        List<Representation> representationVersions = cassandraRecordService.listRepresentationVersions("globalId",
            "dc");
        assertThat(representationVersions, is(Arrays.asList(r3, r2)));
    }


    @Test
    public void shouldDeleteAllRepresentationVersions()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r1 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
        Representation r2 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
        Representation r3 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

        cassandraRecordService.deleteRepresentation("globalId", "dc");

        assertTrue(cassandraRecordService.listRepresentationVersions("globalId", "dc").isEmpty());
    }


    @Test
    public void shouldDeleteAllRecord()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        // given
        Representation dc = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
        Representation jpg = insertDummyPersistentRepresentation("globalId", "jpg", providerId);
        Representation edm1 = cassandraRecordService.createRepresentation("globalId", "edm", providerId);
        Representation edm2 = insertDummyPersistentRepresentation("globalId", "edm", providerId);

        // when
        cassandraRecordService.deleteRecord("globalId");

        // then
        assertTrue(cassandraRecordService.listRepresentationVersions("globalId", "dc").isEmpty());
    }


    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotDeletePersistentRepresentation()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r = insertDummyPersistentRepresentation("globalId", "dc", providerId);
        cassandraRecordService.deleteRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
    }


    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotAddFileToPersistentRepresentation()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r = insertDummyPersistentRepresentation("globalId", "dc", providerId);
        byte[] dummyContent = { 1, 2, 3 };
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent(r.getRecordId(), r.getSchema(), r.getVersion(), f, new ByteArrayInputStream(
                dummyContent));
    }


    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotRemoveFileFromPersistentRepresentation()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r = insertDummyPersistentRepresentation("globalId", "dc", providerId);

        File f = r.getFiles().get(0);
        cassandraRecordService.deleteContent(r.getRecordId(), r.getSchema(), r.getVersion(), f.getFileName());
    }


    @Test(expected = CannotPersistEmptyRepresentationException.class)
    public void shouldNotPersistRepresentationWithoutFile()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r = cassandraRecordService.createRepresentation("globalId", "edm", providerId);
        cassandraRecordService.persistRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
    }


    @Test
    public void shouldPutAndGetFile()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r = cassandraRecordService.createRepresentation("globalId", "edm", providerId);

        byte[] dummyContent = { 1, 2, 3 };
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent(r.getRecordId(), r.getSchema(), r.getVersion(), f, new ByteArrayInputStream(
                dummyContent));

        r = cassandraRecordService.getRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
        assertThat(r.getFiles().size(), is(1));
        File fetchedFile = r.getFiles().get(0);
        assertThat(fetchedFile.getFileName(), is(f.getFileName()));
        assertThat(fetchedFile.getMimeType(), is(f.getMimeType()));
        assertThat(fetchedFile.getContentLength(), is((long) dummyContent.length));
        String contentMd5 = Hashing.md5().hashBytes(dummyContent).toString();
        assertThat(fetchedFile.getMd5(), is(contentMd5));
    }


    @Test
    public void shouldGetContent()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r = cassandraRecordService.createRepresentation("globalId", "edm", providerId);

        byte[] dummyContent = { 1, 2, 3 };
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent(r.getRecordId(), r.getSchema(), r.getVersion(), f, new ByteArrayInputStream(
                dummyContent));
        ByteArrayOutputStream baos = new ByteArrayOutputStream(dummyContent.length);
        cassandraRecordService.getContent(r.getRecordId(), r.getSchema(), r.getVersion(), f.getFileName(), baos);

        assertThat(baos.toByteArray(), is(dummyContent));
    }


    @Test
    public void shouldRemoveFile()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        // given
        Representation r = cassandraRecordService.createRepresentation("globalId", "edm", providerId);

        byte[] dummyContent = { 1, 2, 3 };
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent(r.getRecordId(), r.getSchema(), r.getVersion(), f, new ByteArrayInputStream(
                dummyContent));

        // when
        cassandraRecordService.deleteContent(r.getRecordId(), r.getSchema(), r.getVersion(), f.getFileName());

        // then
        r = cassandraRecordService.getRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
        assertTrue(r.getFiles().isEmpty());
    }


    public void shouldCopyRepresentation()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r = insertDummyPersistentRepresentation("globalId", "dc", providerId);
        Representation copy = cassandraRecordService.copyRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());

        assertThat(copy, is(r));
        for (File f : r.getFiles()) {
            ByteArrayOutputStream rContent = new ByteArrayOutputStream();
            ByteArrayOutputStream copyContent = new ByteArrayOutputStream();
            cassandraRecordService
                    .getContent(r.getRecordId(), r.getSchema(), r.getVersion(), f.getFileName(), rContent);
            cassandraRecordService.getContent(copy.getRecordId(), copy.getSchema(), copy.getVersion(), f.getFileName(),
                copyContent);
            assertThat(rContent.toByteArray(), is(copyContent.toByteArray()));
        }
    }


    private Representation insertDummyPersistentRepresentation(String cloudId, String schema, String providerId)
            throws Exception {
        makeUISProviderSuccess();
        makeUISSuccess();
        Representation r = cassandraRecordService.createRepresentation(cloudId, schema, providerId);
        byte[] dummyContent = { 1, 2, 3 };
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent(cloudId, schema, r.getVersion(), f, new ByteArrayInputStream(dummyContent));

        return cassandraRecordService.persistRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
    }


    private void makeUISProviderSuccess() {
        Mockito.doReturn(true).when(uisHandler).providerExistsInUIS(Mockito.anyString());
    }


    private void makeUISProviderFailure() {
        Mockito.doReturn(false).when(uisHandler).providerExistsInUIS(Mockito.anyString());
    }


    private void makeUISSuccess()
            throws RecordNotExistsException {

        Mockito.doReturn(true).when(uisHandler).recordExistInUIS(Mockito.anyString());
    }


    private void makeUISFailure()
            throws RecordNotExistsException {
        Mockito.doReturn(false).when(uisHandler).recordExistInUIS(Mockito.anyString());
    }


    private void makeUISThrowRecordNotExist()
            throws RecordNotExistsException {
        Mockito.doThrow(RecordNotExistsException.class).when(uisHandler).recordExistInUIS(Mockito.anyString());
    }


    private void makeUISThrowSystemException()
            throws RecordNotExistsException {
        Mockito.doThrow(SystemException.class).when(uisHandler).recordExistInUIS(Mockito.anyString());
    }
}
