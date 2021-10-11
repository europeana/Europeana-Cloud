package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.test.AbstractSecurityTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

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
    private static final String MIME_TYPE = APPLICATION_OCTET_STREAM_TYPE.toString();

    private static final String READ_PERMISSION = "read";
    private static final String WRITE_PERMISSION = "write";
    private static final String BROKEN_PERMISSION = "sdfas";

    private MultipartFile ANY_DATA;

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
        ANY_DATA=new MockMultipartFile("data",new byte[524288]);
        representation = new Representation();
        representation.setCloudId(GLOBAL_ID);
        representation.setRepresentationName(SCHEMA);
        representation.setVersion(VERSION);

       Mockito.doReturn(representation).when(recordService)
               .createRepresentation(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }


    @Test
    public void testSpringPermissionStrings() {

        assertEquals(READ_PERMISSION, Permission.READ.getValue());
        assertEquals(WRITE_PERMISSION, Permission.WRITE.getValue());
    }

    /**
     * Tests giving read access to specific user.
     */
    @Test
    public void vanPersieShouldBeAbleToGetRonaldosFilesAfterAccessWasGivenToHim() throws IOException, RepresentationNotExistsException,
            CannotModifyPersistentRepresentationException, FileAlreadyExistsException,
            FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException {

        Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        login(RONALDO, RONALD_PASSWORD);

        representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, null);
        filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);

        File f = new File();
        f.setFileName(FILE_NAME);
        f.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());
        Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
        ResponseEntity<?> response = fileAuthorizationResource.updateAuthorization(GLOBAL_ID, SCHEMA, VERSION, READ_PERMISSION + "", VAN_PERSIE);

        Assert.assertEquals(response.getStatusCodeValue(), Response.Status.OK.getStatusCode());

        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
    }

    /**
     * Tests giving write access to specific user.
     */
    @Test
    public void vanPersieShouldBeAbleToModifyRonaldosFilesAfterAccessWasGivenToHim() throws IOException, RepresentationNotExistsException,
            CannotModifyPersistentRepresentationException, FileAlreadyExistsException,
            FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException {

        Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        login(RONALDO, RONALD_PASSWORD);

        representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, null);
        filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);

        File f = new File();
        f.setFileName(FILE_NAME);
        f.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());
        Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
        ResponseEntity<?> response = fileAuthorizationResource.updateAuthorization(GLOBAL_ID, SCHEMA, VERSION, WRITE_PERMISSION + "", VAN_PERSIE);

        Assert.assertEquals(response.getStatusCodeValue(), Response.Status.OK.getStatusCode());

        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        fileResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), MIME_TYPE, new ByteArrayInputStream(ANY_DATA.getBytes()));
    }

    /**
     * Tests giving write access to specific user.
     */
    @Test
    public void updateAuthorization_throwsMCSException() throws IOException, RepresentationNotExistsException,
            CannotModifyPersistentRepresentationException, FileAlreadyExistsException,
            FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException {
        //given
        Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        login(RONALDO, RONALD_PASSWORD);
        representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, null);
        filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
        File f = new File();
        f.setFileName(FILE_NAME);
        f.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());
        Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
        try {
            //when
            fileAuthorizationResource.updateAuthorization(GLOBAL_ID, SCHEMA, VERSION, BROKEN_PERMISSION + "", VAN_PERSIE);
            fail("Expected WebApplicationException");
        } catch (WebApplicationException e) {
            //then
            Assert.assertThat(e.getResponse().getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
        }
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        assertUserDontHaveAccessToFile();
    }

    private void assertUserDontHaveAccessToFile() throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException, FileNotExistsException {
        try {
            fileResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), MIME_TYPE, new ByteArrayInputStream(ANY_DATA.getBytes()));
            fail("Expected AccessDeniedException");
        } catch (AccessDeniedException e) {

        }
    }

    // TEST giving access to everyone + anonymous users //

    @Test
    public void randomPersonShouldBeAbleToGetRonaldosFilesAfterAccessWasGivenForEveryone()
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            FileAlreadyExistsException, FileNotExistsException,
            WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException {

        Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        login(RONALDO, RONALD_PASSWORD);

        representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, null);
        filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);

        File f = new File();
        f.setFileName(FILE_NAME);
        f.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());
        Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
        fileAuthorizationResource.giveReadAccessToEveryone(GLOBAL_ID, SCHEMA, VERSION);

        login(RANDOM_PERSON, RANDOM_PASSWORD);
        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
    }

    @Test
    public void unknownUserShouldBeAbleToGetFileAfterAccessWasGivenForEveryone()
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException, FileAlreadyExistsException,
            FileNotExistsException, WrongContentRangeException,
            RecordNotExistsException, ProviderNotExistsException {

        Mockito.doThrow(new FileNotExistsException()).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        login(RONALDO, RONALD_PASSWORD);

        representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, null);
        filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);

        File f = new File();
        f.setFileName(FILE_NAME);
        f.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());
        Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
        fileAuthorizationResource.giveReadAccessToEveryone(GLOBAL_ID, SCHEMA, VERSION);

        login(ANONYMOUS, ANONYMOUS_PASSWORD);
        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
    }

    /*
     * Removing permissions
     */
    @Test(expected = AuthenticationCredentialsNotFoundException.class)
    public void notLoggedInUserShouldNotBeAbleToRemovePrivilegesFromAnyResource() {
        fileAuthorizationResource.removePermissions("someID", "someSchema", "someVersion", READ_PERMISSION + "", "userName");
    }

    @Test(expected = AccessDeniedException.class)
    public void anonymousUserShouldNotBeAbleToRemovePrivilegesFromAnyResource() {
        login(ANONYMOUS, ANONYMOUS_PASSWORD);
        fileAuthorizationResource.removePermissions(GLOBAL_ID, SCHEMA, VERSION, READ_PERMISSION + "", "userName");
    }

    @Test(expected = AccessDeniedException.class)
    public void userWithoutSufficientRightsShouldNotBeAbleToRemovePrivilegesFromAnyResource() {
        login(RONALDO, RONALD_PASSWORD);
        fileAuthorizationResource.removePermissions(GLOBAL_ID, SCHEMA, VERSION, READ_PERMISSION + "", "userName");
    }

    @Test(expected = AccessDeniedException.class)
    public void ronaldoShouldBeAbleToDeletePermissionsForVanPersieToHisFile() throws IOException, RepresentationNotExistsException,
            CannotModifyPersistentRepresentationException, FileAlreadyExistsException,
            FileNotExistsException, WrongContentRangeException, RecordNotExistsException, ProviderNotExistsException {

		/* Add file to eCloud */
        login(RONALDO, RONALD_PASSWORD);
        representationResource.createRepresentation(URI_INFO, GLOBAL_ID, SCHEMA, PROVIDER_ID, null);
        filesResource.sendFile(URI_INFO, GLOBAL_ID, SCHEMA, VERSION, MIME_TYPE, ANY_DATA, FILE_NAME);
        /* Grant access to this file for Van Persie */
        ResponseEntity<?> response = fileAuthorizationResource.updateAuthorization(GLOBAL_ID, SCHEMA, VERSION, READ_PERMISSION + "", VAN_PERSIE);

        Assert.assertEquals(response.getStatusCodeValue(), Response.Status.OK.getStatusCode());

        File f = new File();
        f.setFileName(FILE_NAME);
        f.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());
        Mockito.doReturn(f).when(recordService).getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

//        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, FILE_NAME, null);

        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        /* Check if Van Persie has access to file */
        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);

		/* Delete permissions for Var Persie */
        login(RONALDO, RONALD_PASSWORD);

        response = fileAuthorizationResource.removePermissions(GLOBAL_ID, SCHEMA, VERSION, READ_PERMISSION + "", VAN_PERSIE);

        Assert.assertEquals(response.getStatusCodeValue(), Response.Status.NO_CONTENT.getStatusCode());

		/* VAn Persie should not be able to access file */
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

        fileResource.getFile(GLOBAL_ID, SCHEMA, VERSION, prepareRequestMock(FILE_NAME), null);
    }

    private HttpServletRequest prepareRequestMock(String fileName) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/files/" + fileName);
        return request;
    }
}
