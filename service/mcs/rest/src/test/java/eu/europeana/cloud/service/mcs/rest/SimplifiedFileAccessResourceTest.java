package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static eu.europeana.cloud.service.mcs.rest.AbstractResourceTest.mockHttpServletRequest;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:filesAccessContext.xml"})
public class SimplifiedFileAccessResourceTest {

    @Autowired
    private SimplifiedFileAccessResource fileAccessResource;

    @Autowired
    private RecordService recordService;

    @Autowired
    private UISClient uisClient;

    @Autowired
    private PermissionEvaluator permissionEvaluator;

    private static final String EXISTING_PROVIDER_ID = "existingProviderId";
    private static final String NOT_EXISTING_PROVIDER_ID = "notExistingProviderId";
    private static final String EXISTING_LOCAL_ID = "existingLocalId";
    private static final String NOT_EXISTING_LOCAL_ID = "notExistingLocalId";
    private static String existingLocalIdForRecordWithoutPersistentRepresentation = "existingLocalIdWithoutPersistentRepresentations";
    private static String EXISTING_CLOUD_ID = "existingGlobalId";
    private static String EXISTING_CLOUD_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION = "existingGlobalIdWithoutPersistentRepresentations";
    private static String EXISTING_CLOUD_ID_FOR_RECORD_WITH_ACCESS_RIGHTS = "existingGlobalIdWithAccessRights";
    private static String EXISTING_LOCAL_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION = "existingLocalIdWithoutPersistentRepresentations";
    private static String notExistingGlobalId = "notExistingGlobalId";
    private static String existingRepresentationName = "existingRepresentationName";
    private static final String NOT_EXISTING_REPRESENTATION_NAME = "notExistingRepresentationName";
    
    private HttpServletRequest URI_INFO; /****/

    private static boolean setUpIsDone = false;

    @Before
    public void init() throws CloudException, RepresentationNotExistsException, FileNotExistsException {
        if (setUpIsDone) {
            return;
        }
        setupUisClient();
        setupRecordService();
        setupPermissionEvaluator();
        setupAuthentication();

        setUpIsDone = true;
    }

    @Test(expected = RecordNotExistsException.class)
    public void exceptionShouldBeThrownWhenProviderIdDoesNotExist() throws RecordNotExistsException, DatabaseConnectionException, FileNotExistsException, CloudException, RecordDatasetEmptyException, ProviderDoesNotExistException, WrongContentRangeException, RepresentationNotExistsException, RecordDoesNotExistException, ProviderNotExistsException {
        fileAccessResource.getFile(null, NOT_EXISTING_PROVIDER_ID, NOT_EXISTING_LOCAL_ID, "repName", "fileName");
    }

    @Test(expected = RecordNotExistsException.class)
    public void exceptionShouldBeThrownWhenLocalIdDoesNotExist() throws RecordNotExistsException, DatabaseConnectionException, FileNotExistsException, CloudException, RecordDatasetEmptyException, ProviderDoesNotExistException, WrongContentRangeException, RepresentationNotExistsException, RecordDoesNotExistException, ProviderNotExistsException {
        fileAccessResource.getFile(null, EXISTING_PROVIDER_ID, NOT_EXISTING_LOCAL_ID, "repName", "fileName");
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void exceptionShouldBeThrownWhenRepresentationIsMissing() throws RecordNotExistsException, DatabaseConnectionException, FileNotExistsException, CloudException, RecordDatasetEmptyException, ProviderDoesNotExistException, WrongContentRangeException, RepresentationNotExistsException, RecordDoesNotExistException, ProviderNotExistsException {
        fileAccessResource.getFile(null, EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID, NOT_EXISTING_REPRESENTATION_NAME, "fileName");
    }


    @Test(expected = RepresentationNotExistsException.class)
    public void exceptionShouldBeThrownWhenThereIsNoPersistentRepresentationInGivenRecord() throws RecordNotExistsException, DatabaseConnectionException, FileNotExistsException, CloudException, RecordDatasetEmptyException, ProviderDoesNotExistException, WrongContentRangeException, RepresentationNotExistsException, RecordDoesNotExistException, ProviderNotExistsException {
        fileAccessResource.getFile(null, EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION, existingRepresentationName, "fileName");
    }

    @Test
    public void fileShouldBeReadSuccessfully() throws IOException, RecordNotExistsException, DatabaseConnectionException, FileNotExistsException, CloudException, RecordDatasetEmptyException, ProviderDoesNotExistException, WrongContentRangeException, RepresentationNotExistsException, RecordDoesNotExistException, ProviderNotExistsException, URISyntaxException {
        setupUriInfo();
        ResponseEntity<?> response = fileAccessResource.getFile(URI_INFO, EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID, existingRepresentationName, "fileWithoutReadRights");
        Assert.assertEquals(response.getStatusCodeValue(), 200);
        response.toString();
    }
    
    @Test
    public void fileHeadersShouldBeReadSuccessfully() throws CloudException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException, RepresentationNotExistsException, URISyntaxException {
        setupUriInfo();
        ResponseEntity<?> response = fileAccessResource.getFileHeaders(URI_INFO, EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID, existingRepresentationName, "fileWithoutReadRights");
        Assert.assertEquals(response.getStatusCodeValue(), 200);
        Assert.assertNotNull(response.getHeaders().get("Location"));
        response.toString();
    }
    

    /////////////////////
    //
    ////////////////////

    private void setupUisClient() throws CloudException {
        LocalId localId = new LocalId();

        //rep
        localId.setProviderId("ExistingProviderId");
        localId.setRecordId("ExistingLocalId");

        CloudId cid = new CloudId();
        cid.setId(EXISTING_CLOUD_ID);
        cid.setLocalId(localId);
        //
        CloudId cid2 = new CloudId();
        cid2.setId(EXISTING_CLOUD_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION);
        cid2.setLocalId(localId);

        Mockito.when(uisClient.getCloudId(NOT_EXISTING_PROVIDER_ID, NOT_EXISTING_LOCAL_ID)).thenThrow(new CloudException("", new RecordDoesNotExistException(new ErrorInfo())));
        Mockito.when(uisClient.getCloudId(EXISTING_PROVIDER_ID, NOT_EXISTING_LOCAL_ID)).thenThrow(new CloudException("", new RecordDoesNotExistException(new ErrorInfo())));
        Mockito.when(uisClient.getCloudId(EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID)).thenReturn(cid);
        Mockito.when(uisClient.getCloudId(EXISTING_PROVIDER_ID, EXISTING_LOCAL_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION)).thenReturn(cid2);
        Mockito.when(uisClient.getCloudId("NotExistingProviderId", NOT_EXISTING_LOCAL_ID)).thenThrow(new CloudException("", new RecordDoesNotExistException(new ErrorInfo())));
    }

    private void setupRecordService() throws RepresentationNotExistsException, FileNotExistsException {

        List<Representation> representationsList = new ArrayList<>(1);
        Representation r1 = new Representation();
        r1.setPersistent(true);
        r1.setVersion("123");
        r1.setRepresentationName(existingRepresentationName);
        r1.setCloudId("sampleCloudID");
        representationsList.add(r1);
        //
        List<Representation> representationsListWithoutPersistentRepresentations = new ArrayList<>(1);
        Representation r2 = new Representation();
        r2.setPersistent(false);
        representationsListWithoutPersistentRepresentations.add(r2);
        //
        Mockito.when(recordService.listRepresentationVersions(EXISTING_CLOUD_ID, NOT_EXISTING_REPRESENTATION_NAME)).thenThrow(RepresentationNotExistsException.class);
        Mockito.when(recordService.listRepresentationVersions(EXISTING_CLOUD_ID, existingRepresentationName)).thenReturn(representationsList);
        Mockito.when(recordService.listRepresentationVersions(EXISTING_CLOUD_ID_FOR_RECORD_WITHOUT_PERSISTENT_REPRESENTATION, existingRepresentationName)).thenReturn(representationsListWithoutPersistentRepresentations);
        //
        File file = new File();
        file.setFileName("sampleFileName");
        Mockito.when(recordService.getFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(file);
    }

    private void setupPermissionEvaluator() {
        String targetId = EXISTING_CLOUD_ID + "/" + existingRepresentationName + "/" + 123;
        
        Mockito.when(permissionEvaluator.hasPermission(Mockito.any(Authentication.class), Mockito.eq(targetId), Mockito.anyString(), Mockito.eq("read"))).thenReturn(true);
    }

    private void setupAuthentication() {
        Authentication auth = new UsernamePasswordAuthenticationToken("admin", "pass");
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
    
    private void setupUriInfo() throws URISyntaxException {
        URI_INFO = mockHttpServletRequest();
       // Mockito.when(URI_INFO.getBaseUriBuilder()).thenReturn(new JerseyUriBuilder());
       // Mockito.when(URI_INFO.resolve(Mockito.any(URI.class))).thenReturn(new URI("sdfsdfd"));
    }
}
