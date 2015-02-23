package eu.europeana.cloud.service.mcs.rest;

import java.net.URI;
import java.util.List;

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
import static org.junit.Assert.assertEquals;


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
	private static final String REPRESENTATION_NO_PERMISSIONS_NAME = "REPRESENTATION_NO_PERMISSIONS_NAME";
	
	private static final String COPIED_REPRESENTATION_VERSION = "KIT_KAT_COPIED";
	private static final String REPRESENTATION_NO_PERMISSIONS_FOR_VERSION = "KIT_KAT_NO_PERMISSIONS_FOR";
	
	private UriInfo URI_INFO;
	
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
		
		representation = new Representation();
		representation.setCloudId(GLOBAL_ID);
		representation.setRepresentationName(REPRESENTATION_NAME);
		representation.setVersion(VERSION);

		copiedRepresentation = new Representation();
		copiedRepresentation.setCloudId(GLOBAL_ID);
		copiedRepresentation.setRepresentationName(REPRESENTATION_NAME);
		copiedRepresentation.setVersion(COPIED_REPRESENTATION_VERSION);

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
		Mockito.doReturn(recordWithManyRepresentations).when(recordService).getRecord(Mockito.anyString());
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
	public void shouldBeAbleToGetRepresentationIfHeIsTheOwner() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME);
	}
	
// TODO: At some point there was the idea that Representations have permissions (== they are not visible for everyone)
//	This functionality is no longer there, and the tests are (for now) commented out
//			
	
//	
//	@Test(expected = AccessDeniedException.class)
//	public void shouldThrowExceptionWhenVanPersieTriesToGetRonaldosRepresentations() 
//			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
//				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {
//
//		login(RONALDO, RONALD_PASSWORD);
//		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
//		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
//		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
//		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
//	}
//
//	@Test(expected = AuthenticationCredentialsNotFoundException.class)
//	public void shouldThrowExceptionWhenUnknownUserTriesToGetRepresentation() 
//			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
//				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {
//
//		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
//	}
	

//	public void shouldOnlyGetRepresentationsHeCanReadTest1() throws RecordNotExistsException, ProviderNotExistsException  {
//
//		login(RANDOM_PERSON, RANDOM_PASSWORD);
//		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
//		
//		logoutEveryone();
//		List<Representation> r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
//		
//		assertEquals(r.size(), 0);
//	}
//
//	public void shouldOnlyGetRepresentationsHeCanReadTest2() throws RecordNotExistsException, ProviderNotExistsException  {
//
//		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
//		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
//		List<Representation> r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
//		
//		assertEquals(r.size(), 1);
//	}
//
//	public void shouldOnlyGetRepresentationsHeCanReadTest3() throws RecordNotExistsException, ProviderNotExistsException  {
//
//		Mockito.doReturn(representation)
//			.when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
//
//		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
//		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
//
//		Mockito.doReturn(representationYouDontHavePermissionsFor)
//			.when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
//		
//		login(RONALD_PASSWORD, RONALD_PASSWORD);
//		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
//
//		login(RANDOM_PERSON, RANDOM_PASSWORD);
//		List<Representation> r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
//		assertEquals(r.size(), 0);
//	}
//	
//	public void shouldOnlyGetRepresentationsHeCanReadTest4() throws RecordNotExistsException, ProviderNotExistsException  {
//
//		Mockito.doReturn(representation)
//			.when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
//
//		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
//		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
//
//		Mockito.doReturn(representationYouDontHavePermissionsFor)
//			.when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
//		
//		login(RONALD_PASSWORD, RONALD_PASSWORD);
//		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
//
//		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
//		List<Representation> r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
//		assertEquals(r.size(), 1);
//	}	
	
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
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		representationVersionResource.deleteRepresentation(VERSION, REPRESENTATION_NAME, GLOBAL_ID);
	}
	
	@Test
	public void shouldBeAbleToRecreateDeletedRepresentation() 
			throws RecordNotExistsException, ProviderNotExistsException, 
				RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		representationVersionResource.deleteRepresentation(VERSION, REPRESENTATION_NAME, GLOBAL_ID);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosRepresentations()
			throws RecordNotExistsException, ProviderNotExistsException,
				RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationVersionResource.deleteRepresentation(VERSION, REPRESENTATION_NAME, GLOBAL_ID);
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
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		representationVersionResource.persistRepresentation(URI_INFO, VERSION, REPRESENTATION_NAME, GLOBAL_ID);
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
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		representationVersionResource.copyRepresentation(URI_INFO, VERSION, REPRESENTATION_NAME, GLOBAL_ID);
	}
	
	@Test
	public void shouldBeAbleDeleteCopiedRepresentation() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
			CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		representationVersionResource.copyRepresentation(URI_INFO, VERSION, REPRESENTATION_NAME, GLOBAL_ID);
		
		representationVersionResource.deleteRepresentation(COPIED_REPRESENTATION_VERSION, REPRESENTATION_NAME, GLOBAL_ID);
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
