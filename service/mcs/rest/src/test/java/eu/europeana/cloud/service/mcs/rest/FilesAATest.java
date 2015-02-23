package eu.europeana.cloud.service.mcs.rest;

import java.io.IOException;
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
public class FilesAATest extends AbstractSecurityTest {
	
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
	private RepresentationResource representationResource;

	private static final String FILE_NAME = "FILE_NAME";
	private static final String MIME_TYPE = "CLOWN";
	
	private static final String GLOBAL_ID = "GLOBAL_ID";
	private static final String SCHEMA = "CIRCLE";
	private static final String VERSION = "KIT_KAT";
	
	private static final String PROVIDER_ID = "provider";
	
	private UriInfo URI_INFO;
	private Representation representation;
	private InputStream INPUT_STREAM;

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
	
	private File file;
	
	@Before
	public void mockUp() throws Exception {

		representation = new Representation();
		representation.setCloudId(GLOBAL_ID);
		representation.setRepresentationName(SCHEMA);
		representation.setVersion(VERSION);

		file = new File();
		file.setFileName(FILE_NAME);
		
		URI_INFO = Mockito.mock(UriInfo.class);
		UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);
		
        Mockito.doReturn(uriBuilder).when(URI_INFO).getBaseUriBuilder();
        Mockito.doReturn(uriBuilder).when(uriBuilder).path((Class) Mockito.anyObject());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(URI_INFO).resolve((URI) Mockito.anyObject());

		Mockito.doReturn(representation).when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        
		INPUT_STREAM = new InputStream() {
			
			@Override
			public int read() throws IOException {
				return 0;
			}
		};
	}
	
	// -- GET FILE -- //
	
	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToGetFile() 
			throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException  {
	
		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
	}

	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToGetFile() 
			throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException  {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
	}
	
	@Test
	public void shouldBeAbleToGetFileIfHeIsTheOwner() 
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				FileAlreadyExistsException, FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException  {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
		
		Mockito.doReturn(file).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
	}
	
	// -- ADD FILE -- //

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToAddFile() throws RepresentationNotExistsException,
		CannotModifyPersistentRepresentationException, FileAlreadyExistsException {
	
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, null, FILE_NAME);
	}
	
	@Test
	public void shouldBeAbleToAddFileWhenAuthenticated() throws RepresentationNotExistsException,
		CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(RANDOM_PERSON, RANDOM_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
	}

	// -- DELETE FILE -- //

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteFile() throws RepresentationNotExistsException, 
		FileNotExistsException, CannotModifyPersistentRepresentationException {

		fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME);
	}

	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToDeleteFile() throws RepresentationNotExistsException,
			FileNotExistsException, CannotModifyPersistentRepresentationException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME);
	}
	
	@Test
	public void shouldBeAbleToDeleteFileIfHeIsTheOwner() throws RepresentationNotExistsException, 
			CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
		fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME);
	}
	
	@Test
	public void shouldBeAbleToRecreateDeletedFile() throws RepresentationNotExistsException, 
			CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
		fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosFiles() throws RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(RONALDO, RONALD_PASSWORD);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, INPUT_STREAM, FILE_NAME);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME);
	}
	
	// -- UPDATE FILE -- //

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToUpdateFile() throws RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, FileNotExistsException {

		fileResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, MIME_TYPE, null);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToUpdateFile() throws RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, FileNotExistsException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		fileResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, MIME_TYPE, null);
	}
}
