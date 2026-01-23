package eu.europeana.cloud.service.mcs.controller.aatests;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.controller.RecordsResource;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.test.AbstractSecurityTest;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;

import static org.junit.Assert.assertThrows;

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

    @BeforeEach
  public void mockUp() throws Exception {

    record = new Record();
    record.setCloudId(GLOBAL_ID);
    Mockito.doReturn(record).when(recordService).getRecord(Mockito.anyString());
  }

    @BeforeEach
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


    @Test
    void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteRecord() {
        assertThrows(
                AuthenticationCredentialsNotFoundException.class,
                () -> recordsResource.deleteRecord(GLOBAL_ID)
        );
    }

    @Test
    void shouldThrowExceptionWhenRandomUserTriesToDeleteRecord() {
        login(RANDOM_PERSON, RANDOM_PASSWORD);

        assertThrows(
                AccessDeniedException.class,
                () -> recordsResource.deleteRecord(GLOBAL_ID)
        );
    }

    public void shouldBeAbleToDeleteRecordWhenAdmin()
            throws RecordNotExistsException, RepresentationNotExistsException {
    login(ADMIN, ADMIN_PASSWORD);
    recordsResource.deleteRecord(GLOBAL_ID);
  }
}
