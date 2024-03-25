package eu.europeana.cloud.service.mcs.controller.aatests;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.controller.RecordsResource;
import eu.europeana.cloud.test.AbstractSecurityTest;

import jakarta.validation.constraints.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;

public class RecordResourceAATest extends AbstractSecurityTest {

  @Autowired
  @NotNull
  private RecordsResource recordsResource;

  @Autowired
  @NotNull
  private RecordService recordService;

  private static final String GLOBAL_ID = "GLOBAL_ID";

  private Record record;

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

    record = new Record();
    record.setCloudId(GLOBAL_ID);
    Mockito.doReturn(record).when(recordService).getRecord(Mockito.anyString());
  }

  @Before
  public void init() {
    logoutEveryone();
  }

  /**
   * Makes sure these methods can run even if noone is logged in. No special permissions are required.
   */
  @Test
  public void testMethodsThatDontNeedAnyAuthentication() throws RecordNotExistsException {
    recordsResource.getRecord(URI_INFO, GLOBAL_ID);
  }

  /**
   * Makes sure any random person can just call these methods. No special permissions are required.
   */
  @Test
  public void shouldBeAbleToCallMethodsThatDontNeedAnyAuthenticationWithSomeRandomPersonLoggedIn()
      throws RecordNotExistsException {
    login(RANDOM_PERSON, RANDOM_PASSWORD);
    recordsResource.getRecord(URI_INFO, GLOBAL_ID);
  }

  @Test(expected = AuthenticationCredentialsNotFoundException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteRecord()
      throws RecordNotExistsException, RepresentationNotExistsException {
    recordsResource.deleteRecord(GLOBAL_ID);
  }

  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenRandomUserTriesToDeleteRecord()
      throws RecordNotExistsException, RepresentationNotExistsException {
    login(RANDOM_PERSON, RANDOM_PASSWORD);
    recordsResource.deleteRecord(GLOBAL_ID);
  }

  public void shouldBeAbleToDeleteRecordWhenAdmin()
      throws RecordNotExistsException, RepresentationNotExistsException {
    login(ADMIN, ADMIN_PASSWORD);
    recordsResource.deleteRecord(GLOBAL_ID);
  }
}
