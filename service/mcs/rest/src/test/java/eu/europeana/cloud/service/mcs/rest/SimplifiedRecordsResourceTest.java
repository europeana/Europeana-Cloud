package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriInfo;
import java.util.Arrays;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:recordsAccessContext.xml"})
public class SimplifiedRecordsResourceTest {

    @Autowired
    private SimplifiedRecordsResource recordsResource;

    @Autowired
    private RecordService recordService;

    @Autowired
    private UISClient uisClient;

    private static boolean setUpIsDone = false;

    private static final String PROVIDER_ID = "providerId";
    private static final String NOT_EXISTING_PROVIDER_ID = "notExistingProviderId";
    private static final String LOCAL_ID_FOR_NOT_EXISTING_RECORD = "localIdForNotExistingRecord";
    private static final String LOCAL_ID_FOR_EXISTING_RECORD = "";
    private static final String LOCAL_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS = "localIdForRecordWithoutRepresentations";
    private static final String CLOUD_ID = "cloudId";
    private static final String CLOUD_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS = "cloudIdForRecordsWithoutRepresentations";


    @Before
    public void init() throws CloudException, RecordNotExistsException {
        if (setUpIsDone) {
            return;
        }
        setupUisClient();
        setupRecordService();

        setUpIsDone = true;
    }

    @Test(expected = ProviderNotExistsException.class)
    public void exceptionShouldBeThrowForNotExistingProviderId() throws CloudException, RecordNotExistsException, ProviderNotExistsException {
        recordsResource.getRecord(null, NOT_EXISTING_PROVIDER_ID, "anyLocalId");
    }

    @Test(expected = RecordNotExistsException.class)
    public void exceptionShouldBeThrowForNotExistingCloudId() throws CloudException, RecordNotExistsException, RepresentationNotExistsException, ProviderNotExistsException {
        recordsResource.getRecord(null, PROVIDER_ID, LOCAL_ID_FOR_NOT_EXISTING_RECORD);
    }

    @Test(expected = RecordNotExistsException.class)
    public void exceptionShouldBeThrowForRecordWithoutRepresentations() throws CloudException, RecordNotExistsException, RepresentationNotExistsException, ProviderNotExistsException {
        recordsResource.getRecord(null, PROVIDER_ID, LOCAL_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS);
    }

    @Test
    public void properRecordShouldBeReturned() throws CloudException, RecordNotExistsException, RepresentationNotExistsException, ProviderNotExistsException {
        HttpServletRequest info = Mockito.mock(HttpServletRequest.class);
        //Mockito.when(info.getBaseUriBuilder()).thenReturn(new JerseyUriBuilder());
        //
        Record record = recordsResource.getRecord(info, PROVIDER_ID, LOCAL_ID_FOR_EXISTING_RECORD);

        Assert.assertNotNull(record);
        for (Representation representation : record.getRepresentations()) {
            Assert.assertTrue(representation.getCloudId() == null);
        }
    }

    /////////////
    //
    /////////////
    private void setupUisClient() throws CloudException {
        //
        CloudId cid = new CloudId();
        cid.setId(CLOUD_ID);
        //
        CloudId recordWithoutRepresentations = new CloudId();
        recordWithoutRepresentations.setId(CLOUD_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS);
        LocalId lid = new LocalId();
        lid.setProviderId(PROVIDER_ID);
        lid.setRecordId(LOCAL_ID_FOR_EXISTING_RECORD);
        recordWithoutRepresentations.setLocalId(lid);
        //
        Mockito.when(uisClient.getCloudId(Mockito.eq(NOT_EXISTING_PROVIDER_ID), Mockito.anyString())).thenThrow(new CloudException("", new ProviderDoesNotExistException(new ErrorInfo())));
        Mockito.when(uisClient.getCloudId(PROVIDER_ID, LOCAL_ID_FOR_NOT_EXISTING_RECORD)).thenThrow(new CloudException("", new RecordDoesNotExistException(new ErrorInfo())));
        Mockito.when(uisClient.getCloudId(PROVIDER_ID, LOCAL_ID_FOR_EXISTING_RECORD)).thenReturn(cid);
        Mockito.when(uisClient.getCloudId(PROVIDER_ID, LOCAL_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS)).thenReturn(recordWithoutRepresentations);
    }

    private void setupRecordService() throws RecordNotExistsException {
        Record record = new Record(CLOUD_ID, Arrays.asList(new Representation(CLOUD_ID, "sampleRepName", "sampleVersion", null, null, PROVIDER_ID, null, null, false, null)));
        //
        Mockito.when(recordService.getRecord(CLOUD_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS)).thenThrow(RecordNotExistsException.class);
        Mockito.when(recordService.getRecord(CLOUD_ID)).thenReturn(record);
    }
}
