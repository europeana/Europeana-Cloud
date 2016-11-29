package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.apache.commons.io.input.NullInputStream;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:filesAccessContext.xml"})
public class FileUploadResourceTest {

    @Autowired
    private FileUploadResource fileUploadResource;

    @Autowired
    private RecordService recordService;

    @Autowired
    private PermissionEvaluator permissionEvaluator;

    @Autowired
    private MutableAclService mutableAclService;

    private static boolean setUpIsDone = false;

    private static UriInfo URI_INFO;
    
    private static String CLOUD_ID = "sampleCloudId";
    private static String NON_EXISTING_REPRESENTATION_NAME = "nonExistingRepresentationName";
    private static String EXISTING_PERSISTED_REPRESENTATION_NAME = "existingPersistedRepresentationName";
    
    private Representation createdRepresentation;
    
    @Before
    public void init() throws CloudException, RepresentationNotExistsException, FileNotExistsException, RecordNotExistsException, ProviderNotExistsException, URISyntaxException {
        if (setUpIsDone) {
            return;
        }
        createdRepresentation = new Representation();
        createdRepresentation.setCloudId("123");
        createdRepresentation.setRepresentationName("sampleName");
        createdRepresentation.setVersion("1243");

        UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);
        URI_INFO = Mockito.mock(UriInfo.class);
        Mockito.when(URI_INFO.getBaseUriBuilder()).thenReturn(new JerseyUriBuilder());
        Mockito.when(URI_INFO.resolve(Mockito.any(URI.class))).thenReturn(new URI("sdfsdfd"));
        
        Mockito.doThrow(RepresentationNotExistsException.class).when(recordService).getRepresentation(CLOUD_ID, NON_EXISTING_REPRESENTATION_NAME);
        Mockito.doReturn(createdRepresentation).when(recordService).getRepresentation(CLOUD_ID, EXISTING_PERSISTED_REPRESENTATION_NAME);
        Mockito.doReturn(createdRepresentation).when(recordService).createRepresentation(Mockito.eq(CLOUD_ID), Mockito.eq(NON_EXISTING_REPRESENTATION_NAME), Mockito.anyString());

        setUpIsDone = true;
    }
    
    @Test
    public void shouldUeploadFileForNonExistingRepresentation() throws FileAlreadyExistsException,
            AccessDeniedOrObjectDoesNotExistException, FileNotExistsException, RecordNotExistsException, CannotPersistEmptyRepresentationException, ProviderNotExistsException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
        fileUploadResource.sendFile(URI_INFO, CLOUD_ID, NON_EXISTING_REPRESENTATION_NAME, "fileName", "providerId",
                "mimeType", new NullInputStream(0));
    }
    
}

