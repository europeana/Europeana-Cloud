package eu.europeana.cloud.service.mcs.rest;

import java.io.InputStream;
import java.net.URI;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.test.AbstractSecurityTest;

@RunWith(SpringJUnit4ClassRunner.class)
public class RecordResourceAATest extends AbstractSecurityTest {
	
	@Autowired
	@NotNull
	private RecordsResource recordsResource;
	
	@Autowired
	@NotNull
	private RecordService recordService;

	private static final String GLOBAL_ID = "GLOBAL_ID";
	
	private UriInfo URI_INFO;
	
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
		
		URI_INFO = Mockito.mock(UriInfo.class);
		UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);

        Mockito.doReturn(uriBuilder).when(URI_INFO).getBaseUriBuilder();
        Mockito.doReturn(uriBuilder).when(uriBuilder).path((Class) Mockito.anyObject());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(URI_INFO).resolve((URI) Mockito.anyObject());
        
		Mockito.doReturn(record).when(recordService).getRecord(Mockito.anyString());
	}
	

	/**
	 * Makes sure these methods can run even if noone is logged in.
	 * No special permissions are required.
	 */
	@Test
    public void testMethodsThatDontNeedAnyAuthentication() throws RecordNotExistsException  {

		recordsResource.getRecord(URI_INFO, GLOBAL_ID);
    }
	
	/**
	 * Makes sure any random person can just call these methods.
	 * No special permissions are required.
	 */
	@Test
    public void shouldBeAbleToCallMethodsThatDontNeedAnyAuthenticationWithSomeRandomPersonLoggedIn() 
    		throws RecordNotExistsException  {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		recordsResource.getRecord(URI_INFO, GLOBAL_ID);
    }
	
	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteRecord() 
			throws RecordNotExistsException, RepresentationNotExistsException  {

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
