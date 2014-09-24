package eu.europeana.cloud.service.mcs.rest;

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

import com.google.common.collect.ImmutableList;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.test.AbstractSecurityTest;


@RunWith(SpringJUnit4ClassRunner.class)
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

	private static final String GLOBAL_ID = "GLOBAL_ID";
	private static final String SCHEMA = "CIRCLE";
	private static final String VERSION = "KIT_KAT";
	private static final String PROVIDER_ID = "provider";
	private static final String REPRESENTATION_NAME = "REPRESENTATION_NAME";
	
	private static final String COPIED_REPRESENTATION_VERSION = "KIT_KAT_COPIED";
	
	private UriInfo URI_INFO;
	
	private Record record;
	
	private Representation representation;
	private Representation copiedRepresentation;
	
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
		
		representation = new Representation();
		representation.setCloudId(GLOBAL_ID);
		representation.setRepresentationName(REPRESENTATION_NAME);
		representation.setVersion(VERSION);

		copiedRepresentation = new Representation();
		copiedRepresentation.setCloudId(GLOBAL_ID);
		copiedRepresentation.setRepresentationName(REPRESENTATION_NAME);
		copiedRepresentation.setVersion(COPIED_REPRESENTATION_VERSION);
		
		record = new Record();
		record.setCloudId(GLOBAL_ID);
		record.setRepresentations(ImmutableList.of(representation));
		
		
		URI_INFO = Mockito.mock(UriInfo.class);
		UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);

        Mockito.doReturn(uriBuilder).when(URI_INFO).getBaseUriBuilder();
        Mockito.doReturn(uriBuilder).when(uriBuilder).path((Class) Mockito.anyObject());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(URI_INFO).resolve((URI) Mockito.anyObject());
        
		Mockito.doReturn(representation).when(recordService).getRepresentation(Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(representation).when(recordService).getRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(representation).when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(representation).when(recordService).persistRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(copiedRepresentation).when(recordService).copyRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(record).when(recordService).getRecord(Mockito.anyString());
	}
	
	/**
	 * Makes sure these methods can run even if noone is logged in.
	 * No special permissions are required.
	 */
	@Test
    public void testMethodsThatDontNeedAnyAuthentication() 
    		throws RepresentationNotExistsException, RecordNotExistsException   {

		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
		representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
		representationVersionResource.getRepresentationVersion(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
    }
	
	/**
	 * Makes sure any random person can just call these methods.
	 * No special permissions are required.
	 */
	@Test
    public void shouldBeAbleToCallMethodsThatDontNeedAnyAuthenticationWithSomeRandomPersonLoggedIn() 
    		throws RepresentationNotExistsException, RecordNotExistsException  {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
		representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
    }
	
	@Test
	public void shouldBeAbleToAddRepresentationWhenAuthenticated() 
			throws RecordNotExistsException, ProviderNotExistsException  {
	
		login(RANDOM_PERSON, RANDOM_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
	}
	
	// -- DELETE -- //
	
	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteRepresentation() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		representationVersionResource.deleteRepresentation(VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToDeleteRepresentation() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		representationVersionResource.deleteRepresentation(VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test
	public void shouldBeAbleToDeleteRepresentationIfHeIsTheOwner() 
			throws RecordNotExistsException, ProviderNotExistsException, 
				RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		representationVersionResource.deleteRepresentation(VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test
	public void shouldBeAbleToRecreateDeletedRepresentation() 
			throws RecordNotExistsException, ProviderNotExistsException, 
				RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		representationVersionResource.deleteRepresentation(VERSION, SCHEMA, GLOBAL_ID);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosRepresentations()
			throws RecordNotExistsException, ProviderNotExistsException,
				RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationVersionResource.deleteRepresentation(VERSION, SCHEMA, GLOBAL_ID);
	}

	// -- PERSIST -- //
	
	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToPersistRepresentation() 
			throws RepresentationNotExistsException, 
				CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException  {

		representationVersionResource.persistRepresentation(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToPersistRepresentation() 
			throws RepresentationNotExistsException, 
			CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		representationVersionResource.persistRepresentation(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test
	public void shouldBeAbleToPersistRepresentationIfHeIsTheOwner() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
			CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		representationVersionResource.persistRepresentation(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToPersistRonaldosRepresentations() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationVersionResource.persistRepresentation(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
	}
	
	// -- COPY -- //
	
	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToCopyRepresentation() 
			throws RepresentationNotExistsException, 
				CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException  {

		representationVersionResource.copyRepresentation(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToCopyRepresentation() 
			throws RepresentationNotExistsException, 
			CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		representationVersionResource.copyRepresentation(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test
	public void shouldBeAbleToCopyRepresentationIfHeIsTheOwner() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
			CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		representationVersionResource.copyRepresentation(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test
	public void shouldBeAbleDeleteCopiedRepresentation() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
			CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		representationVersionResource.copyRepresentation(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
		
		representationVersionResource.deleteRepresentation(COPIED_REPRESENTATION_VERSION, SCHEMA, GLOBAL_ID);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToCopyRonaldosRepresentations() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationVersionResource.copyRepresentation(URI_INFO, VERSION, SCHEMA, GLOBAL_ID);
	}
}
