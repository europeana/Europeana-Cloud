package eu.europeana.cloud.service.mcs.controller.aatests;

import static jakarta.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.controller.DataSetsResource;
import eu.europeana.cloud.service.mcs.controller.FileResource;
import eu.europeana.cloud.service.mcs.controller.FilesResource;
import eu.europeana.cloud.service.mcs.controller.RepresentationResource;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.test.AbstractSecurityTest;
import java.io.IOException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.constraints.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.web.multipart.MultipartFile;

public class FilesAATest extends AbstractSecurityTest {

  @Autowired
  @NotNull
  private FileResource fileResource;

  @Autowired
  @NotNull
  private FilesResource filesResource;

  @Autowired
  @NotNull
  private RecordService recordService;

  @Autowired
  @NotNull
  private RepresentationResource representationResource;

  @Autowired
  private DataSetsResource dataSetsResource;

  @Autowired
  private DataSetService dataSetService;

  @Autowired
  private DataSetPermissionsVerifier dataSetPermissionsVerifier;

  private static final String FILE_NAME = "FILE_NAME";
  private static final String FILE_NAME_2 = "FILE_NAME_2";
  private static final String MIME_TYPE = APPLICATION_OCTET_STREAM_TYPE.toString();

  private static final String GLOBAL_ID = "GLOBAL_ID";
  private static final String SCHEMA = "CIRCLE";
  private static final String VERSION = "KIT_KAT";

  private static final String DATASET_NAME = "datasetName";

  private static final String PROVIDER_ID = "provider";

  private Representation representation;
  private MultipartFile ANY_DATA = new MockMultipartFile("data", new byte[1000]);

  /**
   * Pre-defined users
   */
  private final static String RANDOM_PERSON = "Cristiano";
  private final static String RANDOM_PASSWORD = "Ronaldo";

  private final static String VAN_PERSIE = "Robin_Van_Persie";
  private final static String VAN_PERSIE_PASSWORD = "Feyenoord";

  private final static String RONALDO = "Cristiano";
  private final static String RONALD_PASSWORD = "Ronaldo";

  private final static String ADMIN = "admin";
  private final static String ADMIN_PASSWORD = "admin";

  private DataSet testDataSet;

  private File file;
  private File file2;

  @Before
  public void mockUp() throws Exception {

    Mockito.reset(recordService);

    representation = new Representation();
    representation.setCloudId(GLOBAL_ID);
    representation.setRepresentationName(SCHEMA);
    representation.setVersion(VERSION);

    file = new File();
    file.setFileName(FILE_NAME);
    file.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());

    file2 = new File();
    file2.setFileName(FILE_NAME_2);
    file2.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());

    Mockito.doReturn(representation).when(recordService)
           .createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), any());

    testDataSet = new DataSet();
    testDataSet.setId(DATASET_NAME);
    testDataSet.setProviderId(PROVIDER_ID);

    Mockito.doReturn(testDataSet).when(dataSetService).createDataSet(any(), any(), any());
  }

  // -- GET FILE -- //

  @Test(expected = AuthenticationCredentialsNotFoundException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToGetFile()
      throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException {

    fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
  }

  @Test
  public void shouldBeAbleToGetFileIfHeIsTheOwner()
      throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
      FileAlreadyExistsException, FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
    filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);

    Mockito.doReturn(file).when(recordService)
           .getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

    fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
  }

  // -- ADD FILE -- //

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToAddFile() throws IOException, RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, FileAlreadyExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    Mockito.doReturn(false).when(dataSetPermissionsVerifier).isUserAllowedToUploadFileFor(Mockito.any());

    filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, null, FILE_NAME);
  }

  @Test
  public void shouldBeAbleToAddFileWhenAuthenticated() throws IOException, RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    Mockito.doThrow(new FileNotExistsException()).when(recordService)
           .getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

    login(RANDOM_PERSON, RANDOM_PASSWORD);

    Mockito.doReturn(true).when(dataSetPermissionsVerifier).hasReadPermissionFor(Mockito.any());

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
    filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
  }

  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenVanPersieTriesToAddFileToRonaldoRepresentations()
      throws IOException, RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    Mockito.doThrow(new FileNotExistsException()).when(recordService)
           .getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);

    login(RONALDO, RONALD_PASSWORD);
    filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
  }

  // -- DELETE FILE -- //

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteFile() throws RepresentationNotExistsException,
      FileNotExistsException, CannotModifyPersistentRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    Mockito.doReturn(false).when(dataSetPermissionsVerifier).isUserAllowedToDeleteFileFor(Mockito.any());

    fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME));
  }

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenRandomUserTriesToDeleteFile() throws RepresentationNotExistsException,
      FileNotExistsException, CannotModifyPersistentRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    Mockito.doReturn(false).when(dataSetPermissionsVerifier).isUserAllowedToDeleteFileFor(Mockito.any());

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME));
  }

  @Test
  public void shouldBeAbleToDeleteFileIfHeIsTheOwner() throws IOException, RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    Mockito.doThrow(new FileNotExistsException()).when(recordService)
           .getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToUploadFileFor(Mockito.any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToDeleteFileFor(Mockito.any());

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
    filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
    fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME));
  }

  @Test
  public void shouldBeAbleToRecreateDeletedFile() throws IOException, RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    Mockito.doThrow(new FileNotExistsException()).when(recordService)
           .getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToUploadFileFor(Mockito.any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToDeleteFileFor(Mockito.any());

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
    filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
    fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME));
    filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
  }

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosFiles() throws IOException, RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    Mockito.doThrow(new FileNotExistsException()).when(recordService)
           .getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

    login(RONALDO, RONALD_PASSWORD);
    filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME));
  }

  // -- UPDATE FILE -- //

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToUpdateFile() throws RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, FileNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    fileResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), MIME_TYPE, null);
  }

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenRandomUserTriesToUpdateFile() throws RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, FileNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);

    Mockito.doReturn(false).when(dataSetPermissionsVerifier).isUserAllowedToUploadFileFor(Mockito.any());

    fileResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), MIME_TYPE, null);
  }

  private HttpServletRequest prepareRequestMock(String fileName) {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    when(request.getRequestURI()).thenReturn("/files/" + fileName);
    when(request.getRequestURL()).thenReturn(new StringBuffer("/files/" + fileName));
    return request;
  }
}
