package eu.europeana.cloud.service.mcs.rest;

import static org.junit.Assert.assertNotNull;

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
public class RepresentationVersionAuthorizationResourceAATest extends AbstractSecurityTest {
	
	@Autowired
	private RecordsResource recordsResource;
	
	@Autowired
	private RecordService recordService;
	
	@Autowired
	private RepresentationResource representationResource;
	
	@Autowired
	private RepresentationsResource representationsResource;
	
	@Autowired
	private RepresentationVersionResource representationVersionResource;

	@Autowired
	private RepresentationVersionAuthorizationResource representationAuthorizationResource;

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
	
	
	public void unknownUserShouldBeAbleToGetRepresentationAfterAuthorizationHasBeenUpdated() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
		representationAuthorizationResource.giveReadAccessToEveryone(GLOBAL_ID, SCHEMA, VERSION);
		
		logoutEveryone();
		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
	}
	
	@Test
	public void vanPersieShouldBeAbleToGetRonaldosRepresentationAfterAuthorizationHasBeenUpdated() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, 
				CannotPersistEmptyRepresentationException, RecordNotExistsException, ProviderNotExistsException	 {

		login(RONALDO, RONALD_PASSWORD);
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
		representationAuthorizationResource.updateAuthorization(GLOBAL_ID, SCHEMA, VERSION, VAN_PERSIE);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		representationResource.getRepresentation(URI_INFO, GLOBAL_ID, SCHEMA);
	}
}
