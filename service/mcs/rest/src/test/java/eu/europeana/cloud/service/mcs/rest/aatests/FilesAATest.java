package eu.europeana.cloud.service.mcs.rest.aatests;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.rest.FileResource;
import eu.europeana.cloud.service.mcs.rest.FilesResource;
import eu.europeana.cloud.service.mcs.rest.RepresentationResource;
import eu.europeana.cloud.test.AbstractSecurityTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;

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
	private static final String FILE_NAME_2 = "FILE_NAME_2";
	private static final String MIME_TYPE = APPLICATION_OCTET_STREAM_TYPE.toString();
	
	private static final String GLOBAL_ID = "GLOBAL_ID";
	private static final String SCHEMA = "CIRCLE";
	private static final String VERSION = "KIT_KAT";
	
	private static final String PROVIDER_ID = "provider";
	
	private Representation representation;
	private byte[] ANY_DATA = "ANY_DATA".getBytes();

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
	private File file2;
	
	@Before
	public void mockUp() throws Exception {

		Mockito.reset(recordService);
		
		representation = new Representation();
		representation.setCloudId(GLOBAL_ID);
		representation.setRepresentationName(SCHEMA);
		representation.setVersion(VERSION);

		file = new File();
		file.setFileName(FILE_NAME);
		file.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());

		file2 = new File();
		file2.setFileName(FILE_NAME_2);
		file2.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());

		Mockito.doReturn(representation).when(recordService).createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
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
	public void shouldBeAbleToGetAllFilesIfHeIsTheOwner() 
			throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				FileAlreadyExistsException, FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException  {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME_2);
		
		Mockito.doReturn(file).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.same(FILE_NAME));
		Mockito.doReturn(file2).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.same(FILE_NAME_2));
		
		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME_2, null);
	}
	
	
	@Test
	public void shouldBeAbleToGetFileIfHeIsTheOwner() 
			throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
				FileAlreadyExistsException, FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException  {

		
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
		
		Mockito.doReturn(file).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);
	}
	
	// -- ADD FILE -- //

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToAddFile() throws IOException, RepresentationNotExistsException,
		CannotModifyPersistentRepresentationException, FileAlreadyExistsException {
	
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, null, FILE_NAME);
	}
	
	@Test
	public void shouldBeAbleToAddFileWhenAuthenticated() throws IOException, RepresentationNotExistsException,
		CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(RANDOM_PERSON, RANDOM_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToAddFileToRonaldoRepresentations() throws IOException, RepresentationNotExistsException,
		CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);

		login(RONALDO, RONALD_PASSWORD);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
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
	public void shouldBeAbleToDeleteFileIfHeIsTheOwner() throws IOException, RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
		fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME);
	}
	
	@Test
	public void shouldBeAbleToRecreateDeletedFile() throws IOException, RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

		representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
		fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosFiles() throws IOException, RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, FileAlreadyExistsException, FileNotExistsException {

		Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
		
		login(RONALDO, RONALD_PASSWORD);
		filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		fileResource.deleteFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME);
	}
	
	// -- UPDATE FILE -- //

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToUpdateFile() throws IOException, RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, FileNotExistsException {

		fileResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, MIME_TYPE, null);
	}
	
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToUpdateFile() throws IOException, RepresentationNotExistsException,
			CannotModifyPersistentRepresentationException, FileNotExistsException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		fileResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, MIME_TYPE, null);
	}
}
