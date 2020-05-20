package eu.europeana.cloud.service.mcs.rest.aatests;

import com.google.common.collect.ImmutableList;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.config.AuthenticationConfiguration;
import eu.europeana.cloud.service.mcs.config.AuthorizationConfiguration;
import eu.europeana.cloud.service.mcs.config.UnitedExceptionMapper;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.rest.RecordsResource;
import eu.europeana.cloud.service.mcs.rest.RepresentationResource;
import eu.europeana.cloud.service.mcs.rest.RepresentationVersionResource;
import eu.europeana.cloud.service.mcs.rest.RepresentationsResource;
import eu.europeana.cloud.service.mcs.rest.TestServiceConfiguration;
import eu.europeana.cloud.service.mcs.utils.RepresentationsListWrapper;
import eu.europeana.cloud.service.mcs.utils.testcontexts.AbstractResourceTestContext;
import eu.europeana.cloud.service.mcs.utils.testcontexts.CassandraBasedTestContext;
import eu.europeana.cloud.test.AbstractSecurityTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;


//@RunWith(SpringJUnit4ClassRunner.class)

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



		Mockito.doReturn(representation).when(recordService).getRepresentation(Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(representation).when(recordService).getRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(representation).when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(representation).when(recordService).persistRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(copiedRepresentation).when(recordService).copyRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		Mockito.doReturn(record).when(recordService).getRecord(Mockito.anyString());
		Mockito.doReturn(recordWithManyRepresentations).when(recordService).getRecord(Mockito.anyString());
	}

	// -- GET: representationResource -- //

	@Test
	public void shouldBeAbleToGetRepresentationIfHeIsTheOwner()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME);
	}


	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToGetRonaldosRepresentations()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
	}

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenUnknownUserTriesToGetRepresentation()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
	}

	// -- GET: representationVersionResource -- //

	@Test
	public void shouldBeAbleToGetRepresentationVersionIfHeIsTheOwner()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		representationVersionResource.getRepresentationVersion(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
	}

	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToGetRonaldosRepresentationVersion()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationVersionResource.getRepresentationVersion(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
	}

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenUnknownUserTriesToGetRepresentationVersion()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		representationVersionResource.getRepresentationVersion(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
	}



	public void shouldOnlyGetRepresentationsHeCanReadTest1() throws RecordNotExistsException, ProviderNotExistsException  {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);

		logoutEveryone();
		RepresentationsListWrapper r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);

		assertEquals(r.getRepresentations().size(), 0);
	}

	public void shouldOnlyGetRepresentationsHeCanReadTest2() throws RecordNotExistsException, ProviderNotExistsException  {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		RepresentationsListWrapper r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);

		assertEquals(r.getRepresentations().size(), 1);
	}

	public void shouldOnlyGetRepresentationsHeCanReadTest3() throws RecordNotExistsException, ProviderNotExistsException  {

		Mockito.doReturn(representation)
			.when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);

		Mockito.doReturn(representationYouDontHavePermissionsFor)
			.when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

		login(RONALD_PASSWORD, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		RepresentationsListWrapper r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
		assertEquals(r.getRepresentations().size(), 0);
	}

	public void shouldOnlyGetRepresentationsHeCanReadTest4() throws RecordNotExistsException, ProviderNotExistsException  {

		Mockito.doReturn(representation)
			.when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);

		Mockito.doReturn(representationYouDontHavePermissionsFor)
			.when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

		login(RONALD_PASSWORD, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		RepresentationsListWrapper r = representationsResource.getRepresentations(URI_INFO, GLOBAL_ID);
		assertEquals(r.getRepresentations().size(), 1);
	}

	// -- CREATE -- //

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

        representationVersionResource.deleteRepresentation(GLOBAL_ID, SCHEMA, VERSION);
	}

	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToDeleteRepresentation()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		representationVersionResource.deleteRepresentation(GLOBAL_ID, SCHEMA, VERSION);
	}

	@Test
	public void shouldBeAbleToDeleteRepresentationIfHeIsTheOwner()
			throws RecordNotExistsException, ProviderNotExistsException,
				RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
        representationVersionResource.deleteRepresentation(GLOBAL_ID, REPRESENTATION_NAME, VERSION);
	}

	@Test
	public void shouldBeAbleToRecreateDeletedRepresentation()
			throws RecordNotExistsException, ProviderNotExistsException,
				RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
        representationVersionResource.deleteRepresentation(GLOBAL_ID, REPRESENTATION_NAME, VERSION);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
	}

	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosRepresentations()
			throws RecordNotExistsException, ProviderNotExistsException,
				RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationVersionResource.deleteRepresentation(GLOBAL_ID, REPRESENTATION_NAME, VERSION);
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

		representationVersionResource.copyRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
	}

	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToCopyRepresentation()
			throws RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		representationVersionResource.copyRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
	}

	@Test
	public void shouldBeAbleToCopyRepresentationIfHeIsTheOwner()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
			CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		representationVersionResource.copyRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, VERSION);
	}

	@Test
	public void shouldBeAbleDeleteCopiedRepresentation()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
			CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, PROVIDER_ID);
		representationVersionResource.copyRepresentation(URI_INFO, GLOBAL_ID, REPRESENTATION_NAME, VERSION);

		representationVersionResource.deleteRepresentation(GLOBAL_ID, REPRESENTATION_NAME, COPIED_REPRESENTATION_VERSION);
	}

	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToCopyRonaldosRepresentations()
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationVersionResource.copyRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, VERSION);
	}
}
