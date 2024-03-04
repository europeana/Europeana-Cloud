package eu.europeana.cloud.service.uis.security;

import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.rest.DataProviderResource;
import eu.europeana.cloud.service.uis.rest.DataProvidersResource;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import javax.validation.constraints.NotNull;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * DataProviderResource + DataProvidersResource: Authentication - Authorization tests.
 *
 * @author manos
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class DataProviderAATest extends AbstractSecurityTest {

  @Autowired
  @NotNull
  private DataProviderResource dataProviderResource;

  @Autowired
  @NotNull
  private DataProvidersResource dataProvidersResource;

  @Autowired
  @NotNull
  private DataProviderService dataProviderService;

  @Autowired
  @NotNull
  private UniqueIdentifierService uis;

  private UriInfo uriInfo;

  private final static String PROVIDER_ID = "Russell_Stringer_Bell";
  private final static String RECORD_ID = "RECORD_ID";
  private final static String CLOUD_ID = "CLOUD_ID";

  private final static DataProviderProperties DATA_PROVIDER_PROPERTIES = new DataProviderProperties(
      "Name", "Address", "website", "url", "url", "url", "person",
      "remarks");

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

  private CloudId cId;
  private LocalId lId;

  /**
   * Prepare the unit tests
   *
   * @throws CloudIdAlreadyExistException
   * @throws RecordDatasetEmptyException
   * @throws IdHasBeenMappedException
   * @throws CloudIdDoesNotExistException
   * @throws DatabaseConnectionException
   */
  @Before
  public void prepare() throws
      ProviderAlreadyExistsException,
      ProviderDoesNotExistException, URISyntaxException,
      DatabaseConnectionException, CloudIdDoesNotExistException,
      IdHasBeenMappedException, RecordDatasetEmptyException, CloudIdAlreadyExistException {

    DataProvider dp = new DataProvider();
    dp.setId("");
    dp.setProperties(DATA_PROVIDER_PROPERTIES);

    cId = new CloudId();
    cId.setId(CLOUD_ID);

    lId = new LocalId();
    lId.setProviderId(PROVIDER_ID);
    lId.setRecordId(RECORD_ID);
    cId.setLocalId(lId);

    when(
        dataProviderService.createProvider(Mockito.anyString(),
            (DataProviderProperties) Mockito.any())).thenReturn(dp);
    when(
        dataProviderService.updateProvider(Mockito.anyString(),
            (DataProviderProperties) Mockito.any())).thenReturn(dp);

    uriInfo = Mockito.mock(UriInfo.class);
    UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);

    Mockito.doReturn(uriBuilder).when(uriInfo).getBaseUriBuilder();
    Mockito.doReturn(uriBuilder).when(uriInfo).getBaseUriBuilder();

    Mockito.doReturn(uriBuilder).when(uriBuilder).path((Class) Mockito.anyObject());
    Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());

    Mockito.doReturn(new URI("")).when(uriInfo).resolve((URI) Mockito.anyObject());

    when(
        uis.createIdMapping(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(cId);

    when(
        uis.createIdMapping(Mockito.anyString(), Mockito.anyString())).thenReturn(cId);
  }

  /**
   * Makes sure these methods can run even if noone is logged in. No special permissions are required.
   *
   * @throws IdHasBeenMappedException
   * @throws CloudIdDoesNotExistException
   * @throws RecordIdDoesNotExistException
   */
  @Test
  public void testMethodsThatDontNeedAnyAuthentication()
      throws ProviderDoesNotExistException, DatabaseConnectionException,
      RecordDatasetEmptyException, CloudIdDoesNotExistException,
      IdHasBeenMappedException, RecordIdDoesNotExistException,
      CloudIdAlreadyExistException {

    dataProviderResource.getProvider(PROVIDER_ID);
    dataProvidersResource.getProviders(PROVIDER_ID);
  }

  /**
   * Makes sure any random person can just call these methods. No special permissions are required.
   *
   * @throws IdHasBeenMappedException
   * @throws CloudIdDoesNotExistException
   * @throws RecordIdDoesNotExistException
   */
  @Test
  public void shouldBeAbleToCallMethodsThatDontNeedAnyAuthenticationWithSomeRandomPersonLoggedIn()
      throws ProviderDoesNotExistException, DatabaseConnectionException,
      RecordDatasetEmptyException, CloudIdDoesNotExistException,
      IdHasBeenMappedException, RecordIdDoesNotExistException,
      CloudIdAlreadyExistException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    dataProviderResource.getProvider(PROVIDER_ID);
    dataProviderResource.createIdMapping(PROVIDER_ID, CLOUD_ID,
        RECORD_ID);

    dataProvidersResource.getProviders(PROVIDER_ID);
  }

  /**
   * Makes sure that a random person cannot just update a Provider. Simple authentication test to make sure spring security
   * annotations are in place.
   */
  @Test(expected = AccessDeniedException.class)
  public void shouldThrowAccessDeniedExceptionWhenRandomPersonTriesToUpdateProvider()
      throws ProviderDoesNotExistException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    dataProviderResource.updateProvider(DATA_PROVIDER_PROPERTIES, PROVIDER_ID);
  }

  @Test(expected = AuthenticationCredentialsNotFoundException.class)
  public void shouldThrowExceptionWhenUnknowUserTriesToCreateIdMapping()
      throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException,
      ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdAlreadyExistException {

    dataProviderResource.createIdMapping(PROVIDER_ID, CLOUD_ID, RECORD_ID);
  }

  /**
   * Makes sure the person who created a provider has update permissions as well.
   */
  @Test
  public void shouldBeAbleToPerformUpdateIfHeIsAnAdmin()
      throws ProviderDoesNotExistException,
      ProviderAlreadyExistsException, URISyntaxException {

    login(ADMIN, ADMIN_PASSWORD);
    dataProvidersResource.createProvider(mockHttpServletRequest(), DATA_PROVIDER_PROPERTIES,
        PROVIDER_ID);
    dataProviderResource.updateProvider(DATA_PROVIDER_PROPERTIES, PROVIDER_ID);
  }

  /**
   * Makes sure Van Persie cannot update a provider that belongs to Christiano Ronaldo.
   */
  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenVanPersieTriesToUpdateRonaldosStuff()
      throws ProviderDoesNotExistException,
      ProviderAlreadyExistsException, URISyntaxException {

    login(RONALDO, RONALD_PASSWORD);
    dataProvidersResource.createProvider(mockHttpServletRequest(), DATA_PROVIDER_PROPERTIES,
        PROVIDER_ID);
    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    dataProviderResource.updateProvider(DATA_PROVIDER_PROPERTIES, PROVIDER_ID);
  }

  private HttpServletRequest mockHttpServletRequest() {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    when(request.getRequestURL()).thenReturn(new StringBuffer());
    when(request.getHeaderNames()).thenReturn(Collections.enumeration(Collections.emptyList()));
    return request;
  }
}
