package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import java.util.ArrayList;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

public class SimplifiedFileAccessResourceTest extends AbstractResourceTest {

  @Autowired
  private SimplifiedFileAccessResource fileAccessResource;

  @Autowired
  private RecordService recordService;

  @Autowired
  private UISClient uisClient;

  @Autowired
  private PermissionEvaluator permissionEvaluator;

  private static final String EXISTING_PROVIDER_ID = "existingProviderId";
  private static final String NOT_EXISTING_PROVIDER_ID = "notExistingProviderId";
  private static final String EXISTING_LOCAL_ID = "existingLocalId";
  private static final String NOT_EXISTING_LOCAL_ID = "notExistingLocalId";
  private static final String EXISTING_CLOUD_ID = "existingGlobalId";
  private static final String EXISTING_CLOUD_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION = "existingGlobalIdWithoutPersistentRepresentations";
  private static final String EXISTING_LOCAL_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION = "existingLocalIdWithoutPersistentRepresentations";
  private static final String EXISTING_REPRESENTATION_NAME = "existingRepresentationName";
  private static final String NOT_EXISTING_REPRESENTATION_NAME = "notExistingRepresentationName";

  private HttpServletRequest URI_INFO;
  /****/


  @Before
  public void init() throws CloudException, RepresentationNotExistsException, FileNotExistsException {

    setupUisClient();
    setupRecordService();
    setupPermissionEvaluator();
    setupAuthentication();

  }

  @Test(expected = RecordNotExistsException.class)
  public void exceptionShouldBeThrownWhenProviderIdDoesNotExist()
      throws RecordNotExistsException, FileNotExistsException, WrongContentRangeException, RepresentationNotExistsException, ProviderNotExistsException {
    fileAccessResource.getFile(null, NOT_EXISTING_PROVIDER_ID, NOT_EXISTING_LOCAL_ID, "repName", "fileName");
  }

  @Test(expected = RecordNotExistsException.class)
  public void exceptionShouldBeThrownWhenLocalIdDoesNotExist()
      throws RecordNotExistsException, FileNotExistsException, WrongContentRangeException, RepresentationNotExistsException, ProviderNotExistsException {
    fileAccessResource.getFile(null, EXISTING_PROVIDER_ID, NOT_EXISTING_LOCAL_ID, "repName", "fileName");
  }

  @Test(expected = RepresentationNotExistsException.class)
  public void exceptionShouldBeThrownWhenRepresentationIsMissing()
      throws RecordNotExistsException, FileNotExistsException, WrongContentRangeException, RepresentationNotExistsException, ProviderNotExistsException {
    fileAccessResource.getFile(null, EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID, NOT_EXISTING_REPRESENTATION_NAME, "fileName");
  }


  @Test(expected = RepresentationNotExistsException.class)
  public void exceptionShouldBeThrownWhenThereIsNoPersistentRepresentationInGivenRecord()
      throws RecordNotExistsException, FileNotExistsException, WrongContentRangeException, RepresentationNotExistsException, ProviderNotExistsException {
    fileAccessResource.getFile(null, EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION,
        EXISTING_REPRESENTATION_NAME, "fileName");
  }

  @Test
  public void fileShouldBeReadSuccessfully()
      throws RecordNotExistsException, FileNotExistsException, WrongContentRangeException, RepresentationNotExistsException, ProviderNotExistsException {
    setupUriInfo();
    ResponseEntity<?> response = fileAccessResource.getFile(URI_INFO, EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID,
        EXISTING_REPRESENTATION_NAME, "fileWithoutReadRights");
    Assert.assertEquals(200, response.getStatusCodeValue());
    //        response.toString();
  }

  @Test
  public void fileHeadersShouldBeReadSuccessfully()
      throws FileNotExistsException, RecordNotExistsException, ProviderNotExistsException, RepresentationNotExistsException {
    setupUriInfo();
    ResponseEntity<?> response = fileAccessResource.getFileHeaders(URI_INFO, EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID,
        EXISTING_REPRESENTATION_NAME, "fileWithoutReadRights");
    Assert.assertEquals(200, response.getStatusCodeValue());
    Assert.assertNotNull(response.getHeaders().get("Location"));
    //        response.toString();
  }

  /////////////////////
  //
  ////////////////////

  private void setupUisClient() throws CloudException {
    LocalId localId = new LocalId();

    //rep
    localId.setProviderId("ExistingProviderId");
    localId.setRecordId("ExistingLocalId");

    CloudId cid = new CloudId();
    cid.setId(EXISTING_CLOUD_ID);
    cid.setLocalId(localId);
    //
    CloudId cid2 = new CloudId();
    cid2.setId(EXISTING_CLOUD_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION);
    cid2.setLocalId(localId);

    Mockito.doThrow(new CloudException("", new RecordDoesNotExistException(new ErrorInfo()))).when(uisClient)
           .getCloudId(NOT_EXISTING_PROVIDER_ID, NOT_EXISTING_LOCAL_ID);
    Mockito.doThrow(new CloudException("", new RecordDoesNotExistException(new ErrorInfo()))).when(uisClient)
           .getCloudId(EXISTING_PROVIDER_ID, NOT_EXISTING_LOCAL_ID);
    Mockito.doReturn(cid).when(uisClient).getCloudId(EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID);
    Mockito.doReturn(cid2).when(uisClient)
           .getCloudId(EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION);
    Mockito.doThrow(new CloudException("", new RecordDoesNotExistException(new ErrorInfo()))).when(uisClient)
           .getCloudId("NotExistingProviderId", NOT_EXISTING_LOCAL_ID);
  }

  @SuppressWarnings("unchecked")
  private void setupRecordService() throws RepresentationNotExistsException, FileNotExistsException {

    List<Representation> representationsList = new ArrayList<>(1);
    Representation r1 = new Representation();
    r1.setPersistent(true);
    r1.setVersion("123");
    r1.setRepresentationName(EXISTING_REPRESENTATION_NAME);
    r1.setCloudId("sampleCloudID");
    representationsList.add(r1);
    //
    List<Representation> representationsListWithoutPersistentRepresentations = new ArrayList<>(1);
    Representation r2 = new Representation();
    r2.setPersistent(false);
    representationsListWithoutPersistentRepresentations.add(r2);
    //
    Mockito.when(recordService.listRepresentationVersions(EXISTING_CLOUD_ID, NOT_EXISTING_REPRESENTATION_NAME))
           .thenThrow(RepresentationNotExistsException.class);
    Mockito.when(recordService.listRepresentationVersions(EXISTING_CLOUD_ID, EXISTING_REPRESENTATION_NAME))
           .thenReturn(representationsList);
    Mockito.when(recordService.listRepresentationVersions(EXISTING_CLOUD_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION,
        EXISTING_REPRESENTATION_NAME)).thenReturn(representationsListWithoutPersistentRepresentations);
    //
    File file = new File();
    file.setFileName("sampleFileName");
    Mockito.when(recordService.getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
           .thenReturn(file);
  }

  private void setupPermissionEvaluator() {
    String targetId = EXISTING_CLOUD_ID + "/" + EXISTING_REPRESENTATION_NAME + "/" + 123;

    Mockito.when(permissionEvaluator.hasPermission(Mockito.any(Authentication.class), Mockito.eq(targetId), Mockito.anyString(),
        Mockito.eq("read"))).thenReturn(true);
  }

  private void setupAuthentication() {
    Authentication auth = new UsernamePasswordAuthenticationToken("admin", "pass");
    SecurityContextHolder.getContext().setAuthentication(auth);
  }

  private void setupUriInfo() {
    URI_INFO = mockHttpServletRequest();
  }
}
