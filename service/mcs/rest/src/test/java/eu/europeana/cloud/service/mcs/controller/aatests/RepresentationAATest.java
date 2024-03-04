package eu.europeana.cloud.service.mcs.controller.aatests;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

import com.google.common.collect.ImmutableList;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.controller.DataSetsResource;
import eu.europeana.cloud.service.mcs.controller.RecordsResource;
import eu.europeana.cloud.service.mcs.controller.RepresentationResource;
import eu.europeana.cloud.service.mcs.controller.RepresentationVersionResource;
import eu.europeana.cloud.service.mcs.controller.RepresentationsResource;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.service.mcs.utils.RepresentationsListWrapper;
import eu.europeana.cloud.test.AbstractSecurityTest;

import jakarta.validation.constraints.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;


public class RepresentationAATest extends AbstractSecurityTest {

  @Autowired
  @NotNull
  private RecordsResource recordsResource;

  @Autowired
  @NotNull
  private RecordService recordService;

  @Autowired
  @NotNull
  private RepresentationResource representationResource;

  @Autowired
  @NotNull
  private RepresentationsResource representationsResource;

  @Autowired
  @NotNull
  private RepresentationVersionResource representationVersionResource;

  @Autowired
  private DataSetService dataSetService;

  @Autowired
  private DataSetsResource dataSetsResource;

  @Autowired
  private DataSetPermissionsVerifier dataSetPermissionsVerifier;

  private static final String GLOBAL_ID = "GLOBAL_ID";
  private static final String SCHEMA = "CIRCLE";
  private static final String VERSION = "KIT_KAT";
  private static final String PROVIDER_ID = "provider";

  private static final String DATASET_NAME = "datasetName";

  private static final String REPRESENTATION_NAME = "REPRESENTATION_NAME";
  private static final String REPRESENTATION_NO_PERMISSIONS_NAME = "REPRESENTATION_NO_PERMISSIONS_NAME";

  private static final String COPIED_REPRESENTATION_VERSION = "KIT_KAT_COPIED";
  private static final String REPRESENTATION_NO_PERMISSIONS_FOR_VERSION = "KIT_KAT_NO_PERMISSIONS_FOR";

  private Record record;
  private Record recordWithManyRepresentations;

  private Representation representation;
  private Representation copiedRepresentation;
  private Representation representationYouDontHavePermissionsFor;

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

  @Before
  public void mockUp() throws Exception {

    Mockito.reset();

    representation = new Representation();
    representation.setCloudId(GLOBAL_ID);
    representation.setRepresentationName(REPRESENTATION_NAME);
    representation.setVersion(VERSION);

    representationYouDontHavePermissionsFor = new Representation();
    representationYouDontHavePermissionsFor.setCloudId(GLOBAL_ID);
    representationYouDontHavePermissionsFor.setRepresentationName(REPRESENTATION_NO_PERMISSIONS_NAME);
    representationYouDontHavePermissionsFor.setVersion(REPRESENTATION_NO_PERMISSIONS_FOR_VERSION);

    record = new Record();
    record.setCloudId(GLOBAL_ID);
    record.setRepresentations(ImmutableList.of(representation));

    recordWithManyRepresentations = new Record();
    recordWithManyRepresentations.setCloudId(GLOBAL_ID);
    recordWithManyRepresentations.setRepresentations(ImmutableList.of(representation, representationYouDontHavePermissionsFor));

    Mockito.doReturn(representation).when(recordService).getRepresentation(Mockito.anyString(), Mockito.anyString());
    Mockito.doReturn(representation).when(recordService)
           .getRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    Mockito.doReturn(representation).when(recordService)
           .createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any());
    Mockito.doReturn(representation).when(recordService)
           .createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(), any());
    Mockito.doReturn(representation).when(recordService)
           .persistRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    Mockito.doReturn(record).when(recordService).getRecord(Mockito.anyString());
    Mockito.doReturn(recordWithManyRepresentations).when(recordService).getRecord(Mockito.anyString());
  }

  // -- GET: representationResource -- //

  @Test
  public void shouldBeAbleToGetRepresentationIfHeIsTheOwner()
      throws RepresentationNotExistsException,
      RecordNotExistsException, ProviderNotExistsException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    login(RONALDO, RONALD_PASSWORD);

    DataSet d = new DataSet();
    d.setId(DATASET_NAME);
    d.setProviderId(PROVIDER_ID);

    Mockito.doReturn(d).when(dataSetService).createDataSet(any(), any(), any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).hasReadPermissionFor(Mockito.any());

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID, DATASET_NAME, null);
    representationResource.getRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME);
  }


  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenVanPersieTriesToGetRonaldosRepresentations()
      throws RepresentationNotExistsException,
      RecordNotExistsException, ProviderNotExistsException, DataSetNotExistsException, DataSetAssignmentException {

    login(RONALDO, RONALD_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
    representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
  }

  @Test(expected = AuthenticationCredentialsNotFoundException.class)
  public void shouldThrowExceptionWhenUnknownUserTriesToGetRepresentation()
      throws RepresentationNotExistsException {

    representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
  }

  // -- GET: representationVersionResource -- //

  @Test
  public void shouldBeAbleToGetRepresentationVersionIfHeIsTheOwner()
      throws RepresentationNotExistsException,
      RecordNotExistsException, ProviderNotExistsException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    login(RONALDO, RONALD_PASSWORD);

    DataSet d = new DataSet();
    d.setId(DATASET_NAME);
    d.setProviderId(PROVIDER_ID);

    Mockito.doReturn(d).when(dataSetService).createDataSet(any(), any(), any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).hasReadPermissionFor(Mockito.any());

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
    representationVersionResource.getRepresentationVersion(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
  }

  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenVanPersieTriesToGetRonaldosRepresentationVersion()
      throws RepresentationNotExistsException,
      RecordNotExistsException, ProviderNotExistsException, DataSetNotExistsException, DataSetAssignmentException {

    login(RONALDO, RONALD_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    representationVersionResource.getRepresentationVersion(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
  }

  @Test(expected = AuthenticationCredentialsNotFoundException.class)
  public void shouldThrowExceptionWhenUnknownUserTriesToGetRepresentationVersion()
      throws RepresentationNotExistsException {

    representationVersionResource.getRepresentationVersion(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
  }


  public void shouldOnlyGetRepresentationsHeCanReadTest1()
      throws RecordNotExistsException, ProviderNotExistsException, RepresentationNotExistsException, DataSetNotExistsException, DataSetAssignmentException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);

    logoutEveryone();
    RepresentationsListWrapper r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);

    assertEquals(0, r.getRepresentations().size());
  }

  public void shouldOnlyGetRepresentationsHeCanReadTest2()
      throws RecordNotExistsException, ProviderNotExistsException, RepresentationNotExistsException, DataSetNotExistsException, DataSetAssignmentException {

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
    RepresentationsListWrapper r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);

    assertEquals(1, r.getRepresentations().size());
  }

  public void shouldOnlyGetRepresentationsHeCanReadTest3()
      throws RecordNotExistsException, ProviderNotExistsException, DataSetAssignmentException, RepresentationNotExistsException, DataSetNotExistsException {

    Mockito.doReturn(representation).when(recordService)
           .createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any());

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);

    Mockito.doReturn(representationYouDontHavePermissionsFor).when(recordService)
           .createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any());

    login(RONALD_PASSWORD, RONALD_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    RepresentationsListWrapper r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
    assertEquals(0, r.getRepresentations().size());
  }

  public void shouldOnlyGetRepresentationsHeCanReadTest4()
      throws RecordNotExistsException, ProviderNotExistsException, DataSetAssignmentException, RepresentationNotExistsException, DataSetNotExistsException {

    Mockito.doReturn(representation).when(recordService)
           .createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any());

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);

    Mockito.doReturn(representationYouDontHavePermissionsFor).when(recordService)
           .createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any());

    login(RONALD_PASSWORD, RONALD_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    RepresentationsListWrapper r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
    assertEquals(1, r.getRepresentations().size());
  }

  // -- CREATE -- //

  @Test
  public void shouldBeAbleToAddRepresentationWhenAuthenticated()
      throws RecordNotExistsException, ProviderNotExistsException, RepresentationNotExistsException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);

    DataSet d = new DataSet();
    d.setId(DATASET_NAME);
    d.setProviderId(PROVIDER_ID);

    Mockito.doReturn(d).when(dataSetService).createDataSet(any(), any(), any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).hasReadPermissionFor(Mockito.any());

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
  }

  // -- DELETE -- //

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteRepresentation()
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    representationVersionResource.deleteRepresentation(GLOBAL_ID, SCHEMA, VERSION);
  }

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenRandomUserTriesToDeleteRepresentation()
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    Mockito.doReturn(false).when(dataSetPermissionsVerifier).isUserAllowedToDelete(Mockito.any());
    representationVersionResource.deleteRepresentation(GLOBAL_ID, SCHEMA, VERSION);
  }

  @Test
  public void shouldBeAbleToDeleteRepresentationIfHeIsTheOwner()
      throws RecordNotExistsException, ProviderNotExistsException,
      RepresentationNotExistsException, CannotModifyPersistentRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

    DataSet d = new DataSet();
    d.setId(DATASET_NAME);
    d.setProviderId(PROVIDER_ID);

    Mockito.doReturn(d).when(dataSetService).createDataSet(any(), any(), any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToDelete(Mockito.any());

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID, DATASET_NAME, null);
    representationVersionResource.deleteRepresentation(GLOBAL_ID, REPRESENTATION_NAME, VERSION);
  }

  @Test
  public void shouldBeAbleToRecreateDeletedRepresentation()
      throws RecordNotExistsException, ProviderNotExistsException,
      RepresentationNotExistsException, CannotModifyPersistentRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

    DataSet d = new DataSet();
    d.setId(DATASET_NAME);
    d.setProviderId(PROVIDER_ID);

    Mockito.doReturn(d).when(dataSetService).createDataSet(any(), any(), any());
    Mockito.reset(dataSetPermissionsVerifier);
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToDelete(Mockito.any());

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID, DATASET_NAME, null);
    representationVersionResource.deleteRepresentation(GLOBAL_ID, REPRESENTATION_NAME, VERSION);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID, DATASET_NAME, null);
  }

  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosRepresentations()
      throws RecordNotExistsException, ProviderNotExistsException,
      RepresentationNotExistsException, CannotModifyPersistentRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException {

    login(RONALDO, RONALD_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID, DATASET_NAME, null);
    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    representationVersionResource.deleteRepresentation(GLOBAL_ID, REPRESENTATION_NAME, VERSION);
  }

  // -- PERSIST -- //

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToPersistRepresentation()
      throws RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    Mockito.reset(dataSetPermissionsVerifier);
    Mockito.doReturn(false).when(dataSetPermissionsVerifier).hasWritePermissionFor(Mockito.any());

    representationVersionResource.persistRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
  }

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowExceptionWhenRandomUserTriesToPersistRepresentation()
      throws RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    Mockito.reset(dataSetPermissionsVerifier);
    Mockito.doReturn(false).when(dataSetPermissionsVerifier).hasWritePermissionFor(Mockito.any());
    representationVersionResource.persistRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
  }

  @Test
  public void shouldBeAbleToPersistRepresentationIfHeIsTheOwner()
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
      CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException, DataSetAlreadyExistsException {

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

    DataSet d = new DataSet();
    d.setId(DATASET_NAME);
    d.setProviderId(PROVIDER_ID);

    Mockito.doReturn(d).when(dataSetService).createDataSet(any(), any(), any());
    Mockito.reset(dataSetPermissionsVerifier);
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToPersistRepresentation(Mockito.any());

    dataSetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_NAME, "");

    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID, DATASET_NAME, null);
    representationVersionResource.persistRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, VERSION);
  }

  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenVanPersieTriesToPersistRonaldosRepresentations()
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
      CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException, DataSetAssignmentException {

    login(RONALDO, RONALD_PASSWORD);
    representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, DATASET_NAME, null);
    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    representationVersionResource.persistRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
  }

}
