package eu.europeana.cloud.service.uis.security;

import java.net.URI;

import java.net.URISyntaxException;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.rest.DataProviderResource;
import eu.europeana.cloud.service.uis.rest.DataProvidersResource;

/**
 * DataProviderResource + DataProvidersResource: Authentication - Authorization tests.
 * 
 * @author manos
 * 
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
	
	private final static String PROVIDER_ID = "Russell_Stringer_Bell";
	private final static String LOCAL_ID = "LOCAL_ID";
	
	private final static DataProviderProperties DATA_PROVIDER_PROPERTIES = new DataProviderProperties("Name", "Address", "website",
  		"url", "url", "url", "person", "remarks");
	
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

    /**
     * Prepare the unit tests
     */
    @Before
    public void prepare() {
    }

	/**
	 * Makes sure these methods can run even if noone is logged in.
	 * No special permissions are required.
	 * @throws IdHasBeenMappedException 
	 * @throws CloudIdDoesNotExistException 
	 * @throws RecordIdDoesNotExistException 
	 */
	@Test
    public void testMethodsThatDontNeedAnyAuthentication() throws ProviderDoesNotExistException, DatabaseConnectionException, 
    	RecordDatasetEmptyException, CloudIdDoesNotExistException, IdHasBeenMappedException, RecordIdDoesNotExistException {

		dataProviderResource.getProvider(PROVIDER_ID);
		dataProviderResource.getLocalIdsByProvider(PROVIDER_ID, PROVIDER_ID , 100);
		dataProviderResource.getCloudIdsByProvider(PROVIDER_ID, PROVIDER_ID , 100);
		dataProviderResource.createIdMapping(PROVIDER_ID, LOCAL_ID);
		dataProviderResource.removeIdMapping(PROVIDER_ID, LOCAL_ID);
		
		dataProvidersResource.getProviders(PROVIDER_ID);
    }
	
	/**
	 * Makes sure any random person can just call these methods.
	 * No special permissions are required.
	 * @throws IdHasBeenMappedException 
	 * @throws CloudIdDoesNotExistException 
	 * @throws RecordIdDoesNotExistException 
	 */
	@Test
    public void shouldBeAbleToCallMethodsThatDontNeedAnyAuthenticationWithSomeRandomPersonLoggedIn() throws ProviderDoesNotExistException, DatabaseConnectionException, 
    	RecordDatasetEmptyException, CloudIdDoesNotExistException, IdHasBeenMappedException, RecordIdDoesNotExistException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		dataProviderResource.getProvider(PROVIDER_ID);
		dataProviderResource.getLocalIdsByProvider(PROVIDER_ID, PROVIDER_ID , 100);
		dataProviderResource.getCloudIdsByProvider(PROVIDER_ID, PROVIDER_ID , 100);
		dataProviderResource.createIdMapping(PROVIDER_ID, LOCAL_ID);
		dataProviderResource.removeIdMapping(PROVIDER_ID,LOCAL_ID);
		
		dataProvidersResource.getProviders(PROVIDER_ID);
    }

	/**
	 * Makes sure that a random person cannot just update a Provider.
	 * Simple authentication test to make sure spring security annotations are in place.
	 */
	@Test(expected = AccessDeniedException.class)
    public void shouldThrowAccessDeniedExceptionWhenRandomPersonTriesToUpdateProvider() throws ProviderDoesNotExistException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		dataProviderResource.updateProvider(DATA_PROVIDER_PROPERTIES, PROVIDER_ID, null);
    }
	

	/**
	 * Makes sure the person who created a provider has update permissions as well.
	 */
	@Test
    public void shouldBeAbleToPerformUpdateIfHeIsTheOwner() throws ProviderDoesNotExistException, ProviderAlreadyExistsException, URISyntaxException {
		
        DataProvider dp = new DataProvider();
        dp.setId("");
        dp.setProperties(DATA_PROVIDER_PROPERTIES);
		
        Mockito.when(dataProviderService.createProvider(Mockito.anyString(), (DataProviderProperties) Mockito.any())).thenReturn(dp);
        Mockito.when(dataProviderService.updateProvider(Mockito.anyString(), (DataProviderProperties) Mockito.any())).thenReturn(dp);

		UriInfo uriInfo = Mockito.mock(UriInfo.class);
		UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);

        Mockito.doReturn(uriBuilder).when(uriInfo).getBaseUriBuilder();
        Mockito.doReturn(uriBuilder).when(uriBuilder).path((Class) Mockito.anyObject());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        
        Mockito.doReturn(new URI("")).when(uriInfo).resolve((URI) Mockito.anyObject());

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		dataProvidersResource.createProvider(uriInfo, DATA_PROVIDER_PROPERTIES, PROVIDER_ID);
		dataProviderResource.updateProvider(DATA_PROVIDER_PROPERTIES, PROVIDER_ID, uriInfo);
    }
	
	/**
	 * Makes sure Van Persie cannot update a provider that belongs to Christiano Ronaldo.
	 */
	@Test(expected = AccessDeniedException.class)
    public void shouldThrowExceptionWhenVanPersieTriesToUpdateRonaldosStuff() throws ProviderDoesNotExistException, ProviderAlreadyExistsException, URISyntaxException {
		
        DataProvider dp = new DataProvider();
        dp.setId("");
        dp.setProperties(DATA_PROVIDER_PROPERTIES);
		
        Mockito.when(dataProviderService.createProvider(Mockito.anyString(), (DataProviderProperties) Mockito.any())).thenReturn(dp);
        Mockito.when(dataProviderService.updateProvider(Mockito.anyString(), (DataProviderProperties) Mockito.any())).thenReturn(dp);

		UriInfo uriInfo = Mockito.mock(UriInfo.class);
		UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);

        Mockito.doReturn(uriBuilder).when(uriInfo).getBaseUriBuilder();
        Mockito.doReturn(uriBuilder).when(uriBuilder).path((Class) Mockito.anyObject());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        
        Mockito.doReturn(new URI("")).when(uriInfo).resolve((URI) Mockito.anyObject());

		login(RONALDO, RONALD_PASSWORD);
		dataProvidersResource.createProvider(uriInfo, DATA_PROVIDER_PROPERTIES, PROVIDER_ID);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		dataProviderResource.updateProvider(DATA_PROVIDER_PROPERTIES, PROVIDER_ID, uriInfo);
    }
}
