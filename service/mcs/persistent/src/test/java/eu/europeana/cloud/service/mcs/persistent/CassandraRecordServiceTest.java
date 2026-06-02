package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.context.SpiedServicesTestContext;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import eu.europeana.cloud.test.S3TestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

import static eu.europeana.cloud.service.mcs.Storage.DATA_BASE;
import static eu.europeana.cloud.service.mcs.Storage.OBJECT_STORAGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SpiedServicesTestContext.class})
class CassandraRecordServiceTest extends CassandraTestBase {

  //These are version 1 UUIDs with increasing times
  private static final UUID VERSION = UUID.fromString("49cbcaf0-11a3-11f1-a108-977fcbea01cb");
  private static final UUID VERSION_2 = UUID.fromString("49d39320-11a3-11f1-a108-977fcbea01cb");
  private static final UUID VERSION_3 = UUID.fromString("49db3440-11a3-11f1-a108-977fcbea01cb");
  private static final UUID VERSION_4 = UUID.fromString("49e2d560-11a3-11f1-a108-977fcbea01cb");
  private static final UUID VERSION_5 = UUID.fromString("49eac4a0-11a3-11f1-a108-977fcbea01cb");
  private static final UUID VERSION_6 = UUID.fromString("49f265c0-11a3-11f1-a108-977fcbea01cb");
  private static final UUID VERSION_7 = UUID.fromString("49fa06e0-11a3-11f1-a108-977fcbea01cb");
  private static final String DATA_SET_NAME = "dataset1";
  private static final String DATA_SET_DESCRIPTION = "description of this set";

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

  @BeforeAll
  static void setUp() {
    S3TestHelper.startS3MockServer();
  }


  @BeforeEach
  void cleanUpBeforeTest() {
    S3TestHelper.cleanUpBetweenTests();
    Mockito.reset(uisHandler);
  }

  @AfterAll
  static void cleanUp() {
    S3TestHelper.stopS3MockServer();
  }

  @Test
  void shouldCreateAndGetRepresentation() throws Exception {
    mockUISProvider1Success();
    makeUISSuccess();

    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);

    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);

    Representation rFetched = cassandraRecordService.getRepresentation(
        "globalId", "dc", r.getVersion());
    assertThat(rFetched, is(r));
  }

  @Test
  void shouldCreateRepresentationInGivenVersion() throws Exception {
    mockUISProvider1Success();
    makeUISSuccess();

    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);

    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION, DATA_SET_NAME);

    Representation rFetched = cassandraRecordService.getRepresentation(
        "globalId", "dc", VERSION.toString());
    assertThat(rFetched, is(r));
  }

  @Test
  void shouldAllowInvokeCreateRepresentationInTheSameGivenVersionManyTimes() throws Exception {
    mockUISProvider1Success();
    makeUISSuccess();

    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);

    Representation r1 = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION, DATA_SET_NAME);
    Representation r2 = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION, DATA_SET_NAME);

    Representation rFetched = cassandraRecordService.getRepresentation(
        "globalId", "dc", VERSION.toString());

    assertNotNull(r1.getCreationDate());
    assertNotNull(r2.getCreationDate());
    assertNotNull(rFetched.getCreationDate());
    //Setting all dates to the same value, they could differ a bit, and in such case test would fail.
    r1.setCreationDate(rFetched.getCreationDate());
    r2.setCreationDate(rFetched.getCreationDate());

    assertThat(rFetched, is(r1));
    assertThat(rFetched, is(r2));
  }

  @Test
  void shouldThrowExpWhileCreatingRepresentationIfNoRecordInUis()
      throws Exception {

    makeUISThrowIllegalStateException();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    assertThrows(IllegalStateException.class,
        () -> cassandraRecordService.createRepresentation("globalId", "dc",
            PROVIDER_1_ID, VERSION_2, DATA_SET_NAME));
  }

  @Test
  void shouldThrowSystemExpWhileCreatingRepresentationIfUisFails()
      throws Exception {
    mockUISProvider1Success();
    makeUISThrowSystemException();

    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    assertThrows(SystemException.class,
        () -> cassandraRecordService.createRepresentation("globalId", "dc",
            PROVIDER_1_ID, VERSION_2, DATA_SET_NAME));
  }

  @Test
  void shouldThrowExpWhileGettingRecordIfNoRecordInUis() {
    makeUISThrowIllegalStateException();
    assertThrows(IllegalStateException.class,
        () -> cassandraRecordService.getRecord("globalId"));
  }

  @Test
  void shouldThrowSystemExpWhileGettingRecordIfUisFails() {
    makeUISThrowSystemException();
    assertThrows(SystemException.class,
        () -> cassandraRecordService.getRecord("globalId"));
  }

  @Test
  void shouldThrowExpWhileDeletingRecordIfNoRecordInUis() {
    makeUISThrowIllegalStateException();
    assertThrows(IllegalStateException.class,
        () -> cassandraRecordService.deleteRecord("globalId"));
  }

  @Test
  void shouldThrowSystemExpWhileDeletingRecordIfUisFails() {
    makeUISThrowSystemException();
    assertThrows(SystemException.class,
        () -> cassandraRecordService.deleteRecord("globalId"));
  }

  @Test
  void shouldNotGetRepresentationIfNoPersistentExists()
      throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    cassandraRecordService.createRepresentation("globalId", "dc",
        PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    assertThrows(RepresentationNotExistsException.class,
        () -> cassandraRecordService.getRepresentation("globalId", "dc"));
  }

  @Test
  void shouldThrowRepresentationNotFoundExpWhenNoSuchRepresentation() {
    makeUISSuccess();
    assertThrows(RepresentationNotExistsException.class,
        () -> cassandraRecordService.getRepresentation("globalId",
            "not_existing_schema"));
  }

  @Test
  void shouldGetLatestPersistentRepresentation() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    cassandraRecordService.createRepresentation("globalId", "dc",
        PROVIDER_1_ID, VERSION, DATA_SET_NAME);
    insertDummyPersistentRepresentation("globalId", "dc", PROVIDER_1_ID, VERSION_2);
    cassandraRecordService.createRepresentation("globalId", "dc",
        PROVIDER_1_ID, VERSION_3, DATA_SET_NAME);
    Representation r4 = insertDummyPersistentRepresentation("globalId",
        "dc", PROVIDER_1_ID, VERSION_4);
    cassandraRecordService.createRepresentation("globalId", "dc",
        PROVIDER_1_ID, VERSION_5, DATA_SET_NAME);

    Representation rFetched = cassandraRecordService.getRepresentation(
        "globalId", "dc");
    assertThat(rFetched, is(r4));
  }

  @Test
  void shouldNotCreateRepresentationForNotExistingProvider() {
    makeUISFailure();
    makeUISProviderFailure();
    assertThrows(ProviderNotExistsException.class,
        () ->
            cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
                DATA_SET_DESCRIPTION));
    assertThrows(DataSetNotExistsException.class,
        () -> cassandraRecordService.createRepresentation("globalId", "dc",
            "not-existing", VERSION_2, DATA_SET_NAME));
  }

  @Test
  void shouldListAllRepresentationVersionsInOrder() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r1 = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION, DATA_SET_NAME);
    Representation r2 = insertDummyPersistentRepresentation("globalId",
        "dc", PROVIDER_1_ID, VERSION_2);
    Representation r3 = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION_3, DATA_SET_NAME);
    Representation r4 = insertDummyPersistentRepresentation("globalId",
        "dc", PROVIDER_1_ID, VERSION_4);
    Representation r5 = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION_5, DATA_SET_NAME);
    cassandraRecordService.createRepresentation("globalId", "jpg",
        PROVIDER_1_ID, VERSION_6, DATA_SET_NAME);

    List<Representation> representationVersions = cassandraRecordService
        .listRepresentationVersions("globalId", "dc");
    assertThat(representationVersions,
        is(Arrays.asList(r5, r4, r3, r2, r1)));
  }

  @Test
  void shouldReturnWholeRecord()
      throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    // only temp representation
    cassandraRecordService.createRepresentation("globalId", "dc", PROVIDER_1_ID, VERSION, DATA_SET_NAME);

    // only persistent representation
    Representation jpg = insertDummyPersistentRepresentation("globalId", "jpg", PROVIDER_1_ID, VERSION_2);

    // mixture of persistent and temp representations
    cassandraRecordService.createRepresentation("globalId", "edm", PROVIDER_1_ID, VERSION_3, DATA_SET_NAME);
    insertDummyPersistentRepresentation("globalId", "edm", PROVIDER_1_ID, VERSION_4);
    cassandraRecordService.createRepresentation("globalId", "edm", PROVIDER_1_ID, VERSION_5, DATA_SET_NAME);
    Representation edm4 = insertDummyPersistentRepresentation("globalId", "edm", PROVIDER_1_ID, VERSION_6);
    cassandraRecordService.createRepresentation("globalId", "edm", PROVIDER_1_ID, VERSION_7, DATA_SET_NAME);

    Record aRecord = cassandraRecordService.getRecord("globalId");
    Set<Representation> expectedRepresentations = new HashSet<>(Arrays.asList(jpg, edm4));
    Set<Representation> fetchedRepresentations = new HashSet<>(aRecord.getRepresentations());
    assertThat(fetchedRepresentations, is(expectedRepresentations));
  }

  @Test
  void shouldDeleteRepresentationInSpecifiedVersion() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    mockUISProvider2Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r1 = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION, DATA_SET_NAME);
    Representation r2 = insertDummyPersistentRepresentation("globalId",
        "dc", PROVIDER_1_ID, VERSION_2);
    Representation r3 = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION_3, DATA_SET_NAME);
    Representation r4 = cassandraRecordService.createRepresentation(
        "globalId", "dc", PROVIDER_1_ID, VERSION_4, DATA_SET_NAME);

    // when
    cassandraRecordService.deleteRepresentation(r1.getCloudId(),
        r1.getRepresentationName(), r1.getVersion());

    List<Representation> representationVersions = cassandraRecordService
        .listRepresentationVersions("globalId", "dc");
    assertThat(representationVersions, is(Arrays.asList(r4, r3, r2)));
  }

  @Test
  void shouldDeleteAllRepresentationVersions() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    mockUISProvider2Success();
    // given
    final String globalId = "globalId";
    final String represntationName = "dc";
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    cassandraRecordService.createRepresentation(globalId,
        represntationName, PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    insertDummyPersistentRepresentation(globalId, represntationName,
        PROVIDER_1_ID, VERSION);
    cassandraRecordService.createRepresentation(globalId,
        represntationName, PROVIDER_1_ID, VERSION_3, DATA_SET_NAME);

    // when
    cassandraRecordService
        .deleteRepresentation(globalId, represntationName);

    // then
    assertThrows(RepresentationNotExistsException.class,
        () -> cassandraRecordService.listRepresentationVersions(globalId,
            represntationName).isEmpty());
  }


  @Test
  void shouldDeleteAllRepresentationVersionsWhenDeletingRecord() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    mockUISProvider2Success();
    // given
    final String globalId = "globalId";
    final String representationName = "dc";
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    cassandraRecordService.createRepresentation(globalId,
        representationName, PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    insertDummyPersistentRepresentation(globalId, representationName,
        PROVIDER_1_ID, VERSION);
    cassandraRecordService.createRepresentation(globalId,
        representationName, PROVIDER_1_ID, VERSION_3, DATA_SET_NAME);

    // when
    cassandraRecordService
        .deleteRecord(globalId);

    assertThrows(RepresentationNotExistsException.class,
        () -> cassandraRecordService.listRepresentationVersions(globalId,
            representationName).isEmpty());
  }

  @Test
  void shouldDeleteRepresentationRevisionObjectsWhenRecordIsDeleted()
      throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    String cloudId = "cloud-2";

    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    // create new representation
    Representation r = cassandraRecordService.createRepresentation(cloudId,
        "representation-1", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);

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
    cassandraRecordService.insertRepresentationRevision(cloudId, "representation-1", REVISION_PROVIDER, REVISION_NAME,
        r.getVersion(), revision.getCreationTimeStamp());

    // retrieve info from extra table
    List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions(cloudId,
        "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());
    assertThat(representationRevisions.size(), is(1));
    assertThat(representationRevisions.getFirst().getCloudId(), is(r.getCloudId()));
    assertThat(representationRevisions.getFirst().getRepresentationName(), is(r.getRepresentationName()));
    assertThat(RevisionUtils.getRevisionKey(representationRevisions.getFirst().getRevisionProviderId(),
        representationRevisions.getFirst().getRevisionName(),
        representationRevisions.getFirst().getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
    assertThat(representationRevisions.getFirst().getFiles(), is(r.getFiles()));

    cassandraRecordService.deleteRecord(cloudId);

    List<RepresentationRevisionResponse> response = cassandraRecordService.getRepresentationRevisions(cloudId, "representation-1",
        REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());
    assertEquals(0, response.size());
  }

  @Test()
  void shouldDeleteAllRecord() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    mockUISProvider2Success();
    // given
    final String globalId = "globalId";
    final String represntationName1 = "edm";
    final String represntationName2 = "dc";
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    cassandraDataSetService.createDataSet(PROVIDER_2_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    cassandraRecordService.createRepresentation(globalId,
        represntationName2, PROVIDER_1_ID, VERSION, DATA_SET_NAME);
    insertDummyPersistentRepresentation(globalId, "jpg", PROVIDER_1_ID, VERSION_2);
    cassandraRecordService.createRepresentation(globalId,
        represntationName1, PROVIDER_1_ID, VERSION_3, DATA_SET_NAME);
    insertDummyPersistentRepresentation(globalId, represntationName1,
        PROVIDER_1_ID, VERSION_4);

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

  @Test
  void shouldThrowExcWhenDeletingRecordHasNoRepresentations() {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    // record does not have any representation

    // when
    assertThrows(RepresentationNotExistsException.class,
        () -> cassandraRecordService.deleteRecord("globalId"));
    // then should throw RepresentationNotExistsException
  }

  @Test
  void shouldThrowExcWhenDeletingRecordForTheSecondTime()
      throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    cassandraRecordService.createRepresentation("globalId", "dc",
        PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    // delete record
    cassandraRecordService.deleteRecord("globalId");
    // when deleting for the second time
    assertThrows(RepresentationNotExistsException.class,
        () -> cassandraRecordService.deleteRecord("globalId"));

  }

  @Test
  void shouldDeletePersistentRepresentation() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = insertDummyPersistentRepresentation("globalId", "dc", PROVIDER_1_ID, VERSION);
    cassandraRecordService.deleteRepresentation(r.getCloudId(), r.getRepresentationName(), r.getVersion());
    assertThrows(RepresentationNotExistsException.class,
        () -> cassandraRecordService.getRepresentation(r.getCloudId(), r.getRepresentationName(), r.getVersion()));
  }

  @Test
  void shouldNotAddFileToPersistentRepresentation() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = insertDummyPersistentRepresentation("globalId",
        "dc", PROVIDER_1_ID, VERSION);
    byte[] dummyContent = {1, 2, 3};
    File f = new File("content.xml", "application/xml", null, null, 0, null, OBJECT_STORAGE);
    assertThrows(CannotModifyPersistentRepresentationException.class,
        () -> cassandraRecordService.putContent(r.getCloudId(),
            r.getRepresentationName(), r.getVersion(), f,
            new ByteArrayInputStream(dummyContent)));
  }

  @Test
  void shouldNotRemoveFileFromPersistentRepresentation()
      throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = insertDummyPersistentRepresentation("globalId",
        "dc", PROVIDER_1_ID, VERSION);

    File f = r.getFiles().getFirst();
    assertThrows(CannotModifyPersistentRepresentationException.class,
        () -> cassandraRecordService.deleteContent(r.getCloudId(),
            r.getRepresentationName(), r.getVersion(), f.getFileName()));
  }

  @Test
  void shouldNotPersistRepresentationWithoutFile() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    assertThrows(CannotPersistEmptyRepresentationException.class,
        () -> cassandraRecordService.persistRepresentation(r.getCloudId(),
            r.getRepresentationName(), r.getVersion()));
  }

  @Test
  void shouldPutAndGetFile() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);

    byte[] dummyContent = {1, 2, 3};
    File f = new File("content.xml", "application/xml", null, null, 0, null, OBJECT_STORAGE);
    cassandraRecordService.putContent(r.getCloudId(),
        r.getRepresentationName(), r.getVersion(), f,
        new ByteArrayInputStream(dummyContent));

    r = cassandraRecordService.getRepresentation(r.getCloudId(),
        r.getRepresentationName(), r.getVersion());
    assertThat(r.getFiles().size(), is(1));
    File fetchedFile = r.getFiles().getFirst();
    assertThat(fetchedFile.getFileName(), is(f.getFileName()));
    assertThat(fetchedFile.getMimeType(), is(f.getMimeType()));
    assertThat(fetchedFile.getContentLength(),
        is((long) dummyContent.length));
    String contentMd5 = Hashing.md5().hashBytes(dummyContent).toString();
    assertThat(fetchedFile.getMd5(), is(contentMd5));
  }

  @Test
  void shouldPutAndGetFileStoredInDb() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);

    byte[] dummyContent = {1, 2, 3};
    File f = new File("content.xml", "application/xml", null, null, 0, null, DATA_BASE);
    cassandraRecordService.putContent(r.getCloudId(),
        r.getRepresentationName(), r.getVersion(), f,
        new ByteArrayInputStream(dummyContent));

    r = cassandraRecordService.getRepresentation(r.getCloudId(),
        r.getRepresentationName(), r.getVersion());
    assertThat(r.getFiles().size(), is(1));
    File fetchedFile = r.getFiles().getFirst();
    assertThat(fetchedFile.getFileName(), is(f.getFileName()));
    assertThat(fetchedFile.getMimeType(), is(f.getMimeType()));
    assertThat(fetchedFile.getContentLength(),
        is((long) dummyContent.length));
    String contentMd5 = Hashing.md5().hashBytes(dummyContent).toString();
    assertThat(fetchedFile.getMd5(), is(contentMd5));
  }

  @Test
  void shouldGetContent() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);

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
  void shouldRemoveFile() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);

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

  @Test
  void addRevision() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
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
  void addAlreadyExistedRevision() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
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

  @Test
  void addRevisionWithNullRevision() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    assertThrows(RevisionIsNotValidException.class,
        () -> cassandraRecordService.addRevision(r.getCloudId(),
            r.getRepresentationName(), r.getVersion(), null));
  }

  @Test
  void addRevisionWithNullRevisionName() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    Revision revision = new Revision(null, REVISION_PROVIDER);
    assertThrows(RevisionIsNotValidException.class,
        () -> cassandraRecordService.addRevision(r.getCloudId(),
            r.getRepresentationName(), r.getVersion(), revision));
  }

  @Test
  void addRevisionWithNullRevisionProvider() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    Revision revision = new Revision(REVISION_NAME, null);
    assertThrows(RevisionIsNotValidException.class,
        () -> cassandraRecordService.addRevision(r.getCloudId(),
            r.getRepresentationName(), r.getVersion(), revision));
  }

  @Test
  void addRevisionWithNullRevisionCreationDate() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    revision.setCreationTimeStamp(null);
    assertThrows(RevisionIsNotValidException.class,
        () -> cassandraRecordService.addRevision(r.getCloudId(),
            r.getRepresentationName(), r.getVersion(), revision));
  }

  @Test
  void getRevision() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);

    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER, new Date(), true);
    cassandraRecordService.addRevision(r.getCloudId(),
        r.getRepresentationName(), r.getVersion(), revision);
    // then
    String revisionKey = RevisionUtils.getRevisionKey(revision);
    Revision storedRevision = cassandraRecordService.getRevision(r.getCloudId(), r.getRepresentationName(), r.getVersion(),
        revisionKey);
    assertNotNull(storedRevision);
    assertThat(storedRevision, is(revision));

  }


  @Test
  void getNonExistedRevision() throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    // given
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = cassandraRecordService.createRepresentation(
        "globalId", "edm", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);
    // then
    String revisionKey = RevisionUtils.getRevisionKey(REVISION_PROVIDER, REVISION_NAME, new Date().getTime());
    assertThrows(RevisionNotExistsException.class,
        () -> cassandraRecordService
            .getRevision(r.getCloudId(), r.getRepresentationName(), r.getVersion(), revisionKey));
  }

  @Test
  void getRevisionFromNonExistedRepresentation() {
    makeUISSuccess();
    mockUISProvider1Success();
    String revisionKey = RevisionUtils.getRevisionKey(REVISION_PROVIDER, REVISION_NAME, new Date().getTime());
    assertThrows(RepresentationNotExistsException.class,
        () -> cassandraRecordService
            .getRevision("globalId", "not_existing_schema",
                "5573dbf0-5979-11e6-9061-1c6f653f9042", revisionKey));
  }

  private Representation insertDummyPersistentRepresentation(String cloudId,
      String schema, String providerId, UUID version) throws Exception {
    Representation r = cassandraRecordService.createRepresentation(cloudId,
        schema, providerId, version, DATA_SET_NAME);
    byte[] dummyContent = {1, 2, 3};
    File f = new File("content.xml", "application/xml", null, null, 0, null, OBJECT_STORAGE);
    cassandraRecordService.putContent(cloudId, schema, r.getVersion(), f,
        new ByteArrayInputStream(dummyContent));

    return cassandraRecordService.persistRepresentation(r.getCloudId(),
        r.getRepresentationName(), r.getVersion());
  }

  private void mockUISProvider1Success() {
    DataProvider dataProvider1 = new DataProvider();
    dataProvider1.setId(PROVIDER_1_ID);
    dataProvider1.setPartitionKey(PROVIDER_1_PARTITION_KEY);

    Mockito.doReturn(dataProvider1).when(uisHandler)
           .getProvider(PROVIDER_1_ID);
  }

  private void mockUISProvider2Success() {
    DataProvider dataProvider2 = new DataProvider();
    dataProvider2.setId(PROVIDER_2_ID);
    dataProvider2.setPartitionKey(PROVIDER_2_PARTITION_KEY);

    Mockito.doReturn(dataProvider2).when(uisHandler)
           .getProvider(PROVIDER_2_ID);
  }

  private void makeUISProviderFailure() {
    Mockito.doReturn(null).when(uisHandler)
           .getProvider(Mockito.anyString());
  }

  private void makeUISSuccess() {
    Mockito.doReturn(true).when(uisHandler)
           .existsCloudId(Mockito.anyString());
    Mockito.when(uisHandler.existsProvider(Mockito.anyString())).thenReturn(true);
  }

  private void makeUISFailure() {
    Mockito.doReturn(false).when(uisHandler)
           .existsCloudId(Mockito.anyString());
  }

  private void makeUISThrowIllegalStateException() {
    Mockito.doThrow(IllegalStateException.class).when(uisHandler)
           .existsCloudId(Mockito.anyString());
  }

  private void makeUISThrowSystemException() {
    Mockito.doThrow(SystemException.class).when(uisHandler)
           .existsCloudId(Mockito.anyString());
  }


  @Test
  void shouldReturnRepresentationRevisionObjectRevisionLatest()
      throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();

    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);

    // create new representation
    Representation r = cassandraRecordService.createRepresentation("cloud-1",
        "representation-1", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);

    // create and add new revision
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    cassandraRecordService.addRevision(r.getCloudId(),
        r.getRepresentationName(), r.getVersion(), revision);
    Revision revisionLatest = new Revision(REVISION_NAME, REVISION_PROVIDER);
    cassandraRecordService.addRevision(r.getCloudId(),
        r.getRepresentationName(), r.getVersion(), revisionLatest);

    // insert info to extra table
    cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", revision.getRevisionProviderId(),
        revision.getRevisionName(), r.getVersion(), revision.getCreationTimeStamp());
    cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", revisionLatest.getRevisionProviderId(),
        revisionLatest.getRevisionName(), r.getVersion(), revisionLatest.getCreationTimeStamp());

    // retrieve info from extra table
    List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1",
        "representation-1", REVISION_PROVIDER, REVISION_NAME, null);

    assertThat(representationRevisions.size(), is(1));
    assertThat(representationRevisions.getFirst().getCloudId(), is(r.getCloudId()));
    assertThat(representationRevisions.getFirst().getRepresentationName(), is(r.getRepresentationName()));
    assertThat(RevisionUtils.getRevisionKey(representationRevisions.getFirst().getRevisionProviderId(),
        representationRevisions.getFirst().getRevisionName(),
        representationRevisions.getFirst().getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revisionLatest)));
    assertThat(representationRevisions.getFirst().getRevisionTimestamp(), is(revisionLatest.getCreationTimeStamp()));

    // get the other revision
    representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1", "representation-1", REVISION_PROVIDER,
        REVISION_NAME, revision.getCreationTimeStamp());

    assertThat(representationRevisions.size(), is(1));
    assertThat(representationRevisions.getFirst().getCloudId(), is(r.getCloudId()));
    assertThat(representationRevisions.getFirst().getRepresentationName(), is(r.getRepresentationName()));
    assertThat(RevisionUtils.getRevisionKey(representationRevisions.getFirst().getRevisionProviderId(),
        representationRevisions.getFirst().getRevisionName(),
        representationRevisions.getFirst().getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
    assertThat(representationRevisions.getFirst().getRevisionTimestamp(), is(revision.getCreationTimeStamp()));
  }

  @Test
  void shouldReturnRepresentationRevisionObjectRevisionFirst()
      throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();

    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);

    // create new representation
    Representation r = cassandraRecordService.createRepresentation("cloud-1",
        "representation-1", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);

    // create and add new revision
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    cassandraRecordService.addRevision(r.getCloudId(),
        r.getRepresentationName(), r.getVersion(), revision);

    // insert info to extra table
    cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", revision.getRevisionProviderId(),
        revision.getRevisionName(), r.getVersion(), revision.getCreationTimeStamp());

    // retrieve info from extra table
    List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1",
        "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());

    assertThat(representationRevisions.size(), is(1));
    assertThat(representationRevisions.getFirst().getCloudId(), is(r.getCloudId()));
    assertThat(representationRevisions.getFirst().getRepresentationName(), is(r.getRepresentationName()));
    assertThat(RevisionUtils.getRevisionKey(representationRevisions.getFirst().getRevisionProviderId(),
        representationRevisions.getFirst().getRevisionName(),
        representationRevisions.getFirst().getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
    assertThat(representationRevisions.getFirst().getFiles(), is(r.getFiles()));
    assertThat(representationRevisions.getFirst().getFiles().size(), is(0));

    // add files to representation version
    byte[] dummyContent = {1, 2, 3};
    File f = new File("content.xml", "application/xml", null, null, 0, null);
    cassandraRecordService.putContent("cloud-1", "representation-1", r.getVersion(), f,
        new ByteArrayInputStream(dummyContent));

    // retrieve representation again
    r = cassandraRecordService.getRepresentation("cloud-1", "representation-1", r.getVersion());

    // retrieve info from extra table again
    representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1", "representation-1", REVISION_PROVIDER,
        REVISION_NAME, revision.getCreationTimeStamp());
    assertThat(representationRevisions.size(), is(1));
    assertThat(representationRevisions.getFirst().getCloudId(), is(r.getCloudId()));
    assertThat(representationRevisions.getFirst().getRepresentationName(), is(r.getRepresentationName()));
    assertThat(RevisionUtils.getRevisionKey(representationRevisions.getFirst().getRevisionProviderId(),
        representationRevisions.getFirst().getRevisionName(),
        representationRevisions.getFirst().getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
    assertThat(representationRevisions.getFirst().getFiles(), is(r.getFiles()));
  }


  @Test
  void shouldDeleteRepresentationRevisionObjectWhenRepresentationIsDeleted()
      throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();

    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);

    // create new representation
    Representation r = cassandraRecordService.createRepresentation("cloud-1",
        "representation-1", PROVIDER_1_ID, VERSION_2, DATA_SET_NAME);

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
    cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME,
        r.getVersion(), revision.getCreationTimeStamp());

    // retrieve info from extra table
    List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1",
        "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());

    assertThat(representationRevisions.size(), is(1));
    assertThat(representationRevisions.getFirst().getCloudId(), is(r.getCloudId()));
    assertThat(representationRevisions.getFirst().getRepresentationName(), is(r.getRepresentationName()));
    assertThat(RevisionUtils.getRevisionKey(representationRevisions.getFirst().getRevisionProviderId(),
        representationRevisions.getFirst().getRevisionName(),
        representationRevisions.getFirst().getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
    assertThat(representationRevisions.getFirst().getFiles(), is(r.getFiles()));

    cassandraRecordService.deleteRepresentation("cloud-1", "representation-1");

    // retrieve info from extra table again
    List<RepresentationRevisionResponse> response = cassandraRecordService.getRepresentationRevisions("cloud-1",
        "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());
    assertEquals(0, response.size());
  }


  @Test
  void shouldReturnRepresentationRevisionObjectFilesFirst()
      throws Exception {
    makeUISSuccess();
    mockUISProvider1Success();
    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME,
        DATA_SET_DESCRIPTION);
    Representation r = insertDummyPersistentRepresentation("cloud-1", "representation-1", PROVIDER_1_ID, VERSION);
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    cassandraRecordService.addRevision(r.getCloudId(),
        r.getRepresentationName(), r.getVersion(), revision);
    cassandraRecordService.insertRepresentationRevision("cloud-1", "representation-1", REVISION_PROVIDER, REVISION_NAME,
        r.getVersion(), revision.getCreationTimeStamp());
    List<RepresentationRevisionResponse> representationRevisions = cassandraRecordService.getRepresentationRevisions("cloud-1",
        "representation-1", REVISION_PROVIDER, REVISION_NAME, revision.getCreationTimeStamp());

    assertThat(representationRevisions.getFirst().getCloudId(), is(r.getCloudId()));
    assertThat(representationRevisions.getFirst().getRepresentationName(), is(r.getRepresentationName()));
    assertThat(RevisionUtils.getRevisionKey(representationRevisions.getFirst().getRevisionProviderId(),
        representationRevisions.getFirst().getRevisionName(),
        representationRevisions.getFirst().getRevisionTimestamp().getTime()), is(RevisionUtils.getRevisionKey(revision)));
    assertThat(representationRevisions.getFirst().getFiles(), is(r.getFiles()));
  }

  @Test
  void shouldAddAnnotationToRepresentationVersion() throws Exception {

    mockUISProvider1Success();
    makeUISSuccess();

    cassandraDataSetService.createDataSet(PROVIDER_1_ID, DATA_SET_NAME, DATA_SET_DESCRIPTION);

    Representation r = cassandraRecordService.createRepresentation("globalId", "dc", PROVIDER_1_ID, VERSION_2,
            DATA_SET_NAME);

    Annotation annotation = new Annotation(Annotation.AnnotationKey.INVALID, "");

    cassandraRecordService.addAnnotationsToRepresentationVersion(r, Set.of(annotation));

    Representation representation = cassandraRecordService.getRepresentation("globalId", "dc", r.getVersion());
    assertThat(representation.getAnnotations().size(), is(1));
    assertThat(representation.getAnnotations().contains(annotation), is(true));

  }

}
