package eu.europeana.cloud.service.uis.security;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.*;
import eu.europeana.cloud.service.uis.rest.UniqueIdentifierResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * UniqueIdentifierResource: Authentication - Authorization tests.
 *
 * @author manos
 */
@ExtendWith(SpringExtension.class)
public class UisAATest extends AbstractSecurityTest {

  @Autowired
  private UniqueIdentifierService uniqueIdentifierService;

  @Autowired
  private UniqueIdentifierResource uisResource;

  private final static String PROVIDER_ID = "Russell_Stringer_Bell";
  private final static String LOCAL_ID = "LOCAL_ID";
  private final static String RECORD_ID = "RECORD_ID";
  private final static String CLOUD_ID = "CLOUD_ID";

  /**
   * Pre-defined users
   */
  private final static String RANDOM_PERSON = "Cristiano";
  private final static String RANDOM_PASSWORD = "Ronaldo";

  /**
   * Prepare the unit tests
   */
  @BeforeEach
  void prepare() {
  }


  /**
   * Makes sure these methods can run even if noone is logged in. No special permissions are required.
   */
  @Test
  void testMethodsThatDontNeedAnyAuthentication()
          throws DatabaseConnectionException, ProviderDoesNotExistException,
          RecordDatasetEmptyException, CloudIdDoesNotExistException, RecordDoesNotExistException {

    uisResource.getCloudId(PROVIDER_ID, RECORD_ID);
    uisResource.getLocalIds(CLOUD_ID);
  }


  /**
   * Makes sure any random person can just call these methods. No special permissions are required.
   */
  @Test
  void shouldBeAbleToCallMethodsThatDontNeedAnyAuthenticationWithSomeRandomPersonLoggedIn()
          throws DatabaseConnectionException, ProviderDoesNotExistException,
          RecordDatasetEmptyException, CloudIdDoesNotExistException, RecordDoesNotExistException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    uisResource.getCloudId(PROVIDER_ID, RECORD_ID);
    uisResource.getLocalIds(CLOUD_ID);
  }


  @Test
  void shouldBeAbleToCallMethodsThatNeedBasicAuthenticationWithSomeRandomPersonLoggedIn()
          throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException,
          RecordDatasetEmptyException, CloudIdDoesNotExistException, CloudIdAlreadyExistException {

    CloudId cloudId = new CloudId();
    cloudId.setId(CLOUD_ID);
    LocalId localId = new LocalId();
    localId.setRecordId(LOCAL_ID);
    cloudId.setLocalId(localId);

    Mockito.when(uniqueIdentifierService.createCloudId(Mockito.anyString(), Mockito.anyString())).thenReturn(
            cloudId);

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    uisResource.createCloudId(PROVIDER_ID, LOCAL_ID);
  }

  @Test
  void shouldThrowExceptionWhenUnknowUserTriesToCreateCloudID() {

    assertThrows(AuthenticationCredentialsNotFoundException.class, () -> uisResource.createCloudId(PROVIDER_ID, LOCAL_ID));
  }

}
