package eu.europeana.cloud.service.mcs.rest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.cassandra.auth.Permission;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.test.AbstractSecurityTest;


@RunWith(SpringJUnit4ClassRunner.class)
public class RepresentationAuthorizationResourceAATest extends AbstractSecurityTest {
	
	@Autowired
	@NotNull
	private FileResource fileResource;
	
	@Autowired
	@NotNull
	private FilesResource filesResource;
	
    @Autowired
	@NotNull
    private RecordService recordService;

	@Autowired
	@NotNull
	private RepresentationAuthorizationResource fileAuthorizationResource;
	
	@Autowired
	@NotNull
	private RepresentationResource representationResource;

	private static final String GLOBAL_ID = "GLOBAL_ID";
	private static final String SCHEMA = "CIRCLE";
	private static final String VERSION = "KIT_KAT";
	private static final String PROVIDER_ID = "provider";
	private static final String REPRESENTATION_NAME = "REPRESENTATION_NAME";
	
	private static final String COPIED_REPRESENTATION_VERSION = "KIT_KAT_COPIED";

	private static final String FILE_NAME = "FILE_NAME";
	private static final String MIME_TYPE = "CLOWN";
	
	private static final int READ_PERMISSION = 1;
	private static final int WRITE_PERMISSION = 2;
	
	private UriInfo URI_INFO;
	
	private InputStream INPUT_STREAM;
	
	private Representation representation;
	
	/**
	 * Pre-defined users
	 */
	private final static String RANDOM_PERSON = "admin";
	private final static String RANDOM_PASSWORD = "admin";

	private final static String VAN_PERSIE = "Robin_Van_Persie";
	private final static String VAN_PERSIE_PASSWORD = "Feyenoord";

	private final static String RONALDO = "Cristiano";
	private final static String RONALD_PASSWORD = "Ronaldo";

	private final static String ANONYMOUS = "Anonymous";
	private final static String ANONYMOUS_PASSWORD = "Anonymous";
	
	private final static String ADMIN = "admin";
	private final static String ADMIN_PASSWORD = "admin";
	
	@Before
	public void mockUp() throws Exception {

		URI_INFO = Mockito.mock(UriInfo.class);
		UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);
		
		representation = new Representation();
		representation.setCloudId(GLOBAL_ID);
		representation.setRepresentationName(SCHEMA);
		representation.setVersion(VERSION);

		Mockito.doReturn(representation).when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        
        Mockito.doReturn(uriBuilder).when(URI_INFO).getBaseUriBuilder();
        Mockito.doReturn(uriBuilder).when(uriBuilder).path((Class) Mockito.anyObject());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(URI_INFO).resolve((URI) Mockito.anyObject());
        
		INPUT_STREAM = new InputStream() {
			
			@Override
			public int read() throws IOException {
				// TODO Auto-generated method stub
				return 0;
			}
		};
	}
	

	@Test
	public void testSpringPermissionStrings()  {
		
		assertEquals(READ_PERMISSION, BasePermission.READ.getMask());
		assertEquals(WRITE_PERMISSION, BasePermission.WRITE.getMask());
	}
	
	/** 
	 * Tests giving read access to specific user. 
	 */
	@Test
	public void vanPersieShouldBeAbleToGetRonaldosFilesAfterAccessWasGivenToHim() throws RepresentationNotExistsException, 
			CannotModifyPersistentRepresentationException, FileAlreadyExistsException,
			FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException 	 {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(RONALDO, RONALD_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
		
		File f = new File();
		f.setFileName(FILE_NAME);
		Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
		fileAuthorizationResource.updateAuthorization(GLOBAL_ID, SCHEMA, VERSION, VAN_PERSIE, READ_PERMISSION + "");
		
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
	}

	/** 
	 * Tests giving write access to specific user. 
	 */
	@Test
	public void vanPersieShouldBeAbleToModifyRonaldosFilesAfterAccessWasGivenToHim() throws RepresentationNotExistsException, 
			CannotModifyPersistentRepresentationException, FileAlreadyExistsException,
			FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException 	 {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(RONALDO, RONALD_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
		
		File f = new File();
		f.setFileName(FILE_NAME);
		Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
		fileAuthorizationResource.updateAuthorization(GLOBAL_ID, SCHEMA, VERSION, VAN_PERSIE, WRITE_PERMISSION + "");
		
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		fileResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, MIME_TYPE, INPUT_STREAM);
	}
	
	// TEST giving access to everyone + anonymous users //
	
	@Test
	public void randomPersonShouldBeAbleToGetRonaldosFilesAfterAccessWasGivenForEveryone() 
			throws RepresentationNotExistsException,  CannotModifyPersistentRepresentationException,
				FileAlreadyExistsException, FileNotExistsException,
				WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException	 {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(RONALDO, RONALD_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
		
		File f = new File();
		f.setFileName(FILE_NAME);
		Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
		fileAuthorizationResource.giveReadAccessToEveryone(GLOBAL_ID, SCHEMA, VERSION);

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
	}

	@Test
	public void unknownUserShouldBeAbleToGetFileAfterAccessWasGivenForEveryone() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, FileAlreadyExistsException,
				FileNotExistsException, WrongContentRangeException,
				RecordNotExistsException, ProviderNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(RONALDO, RONALD_PASSWORD);
		
		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
		
		File f = new File();
		f.setFileName(FILE_NAME);
		Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
		fileAuthorizationResource.giveReadAccessToEveryone(GLOBAL_ID, SCHEMA, VERSION);
		
		login(ANONYMOUS, ANONYMOUS_PASSWORD);
		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
	}
}
