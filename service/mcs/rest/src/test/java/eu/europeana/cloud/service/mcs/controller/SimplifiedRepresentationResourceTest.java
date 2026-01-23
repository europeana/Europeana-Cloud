package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SimplifiedRepresentationResourceTest extends AbstractResourceTest {

  @Autowired
  private SimplifiedRepresentationResource representationResource;

  @Autowired
  private RecordService recordService;

  @Autowired
  private UISClient uisClient;


  private static final String PROVIDER_ID = "providerId";
  private static final String CLOUD_ID = "cloudId";
  private static final String NOT_EXISTING_PROVIDER_ID = "notExistingProviderId";
  private static final String LOCAL_ID = "localId";
  private static final String LOCAL_ID_FOR_NOT_EXISTING_RECORD = "localIdForNotExistingRecord";
  private static final String EXISTING_REPRESENTATION_NAME = "existingRepresentationName";
    private static final String RANDOM_REPRESENTATION_NAME = "randomRepresentationName";

    @BeforeEach
    public void init() throws CloudException, RepresentationNotExistsException {
        Mockito.reset(uisClient);
        Mockito.reset(recordService);
        setupUisClient();
        setupRecordService();
    }

    @Test
    void exceptionShouldBeThrowForNotExistingProviderId() {
        assertThrows(
                ProviderNotExistsException.class,
                () -> representationResource.getRepresentation(
                        null, NOT_EXISTING_PROVIDER_ID, "localID", "repName")
        );
    }

    @Test
    void exceptionShouldBeThrowForNotExistingCloudId() {
        assertThrows(
                RecordNotExistsException.class,
                () -> representationResource.getRepresentation(
                        null, PROVIDER_ID, LOCAL_ID_FOR_NOT_EXISTING_RECORD, "repName")
        );
    }

    @Test
    void exceptionShouldBeThrowForRecordWithoutNamedRepresentation() {
        assertThrows(
                RepresentationNotExistsException.class,
                () -> representationResource.getRepresentation(
                        null, PROVIDER_ID, LOCAL_ID, RANDOM_REPRESENTATION_NAME)
        );
    }


    @Test
    public void properRepresentationShouldBeReturned()
            throws RepresentationNotExistsException, ProviderNotExistsException, RecordNotExistsException {
        HttpServletRequest info = mockHttpServletRequest();
        //
        Representation rep = representationResource.getRepresentation(info, PROVIDER_ID, LOCAL_ID, EXISTING_REPRESENTATION_NAME);
        //
        assertNotNull(rep);
        assertThat(rep.getCloudId(), is(CLOUD_ID));
    assertThat(rep.getRepresentationName(), is(EXISTING_REPRESENTATION_NAME));
  }

  /////////////
  //
  /////////////
  private void setupUisClient() throws CloudException {
    //
    CloudId cid = new CloudId();
    cid.setId(CLOUD_ID);
    //

    Mockito.when(uisClient.getCloudId(Mockito.eq(NOT_EXISTING_PROVIDER_ID), Mockito.anyString()))
           .thenThrow(new CloudException("", new ProviderDoesNotExistException(new ErrorInfo())));
    Mockito.when(uisClient.getCloudId(PROVIDER_ID, LOCAL_ID_FOR_NOT_EXISTING_RECORD))
           .thenThrow(new CloudException("", new RecordDoesNotExistException(new ErrorInfo())));
    Mockito.when(uisClient.getCloudId(PROVIDER_ID, LOCAL_ID)).thenReturn(cid);
  }

  private void setupRecordService() throws RepresentationNotExistsException {
    Representation rep = new Representation(CLOUD_ID, EXISTING_REPRESENTATION_NAME, "sampleVersion", null, null, PROVIDER_ID,
        null, null, true, null, null);

    Mockito.when(recordService.getRepresentation(CLOUD_ID, EXISTING_REPRESENTATION_NAME)).thenReturn(rep);
    Mockito.when(recordService.getRepresentation(CLOUD_ID, RANDOM_REPRESENTATION_NAME))
           .thenThrow(RepresentationNotExistsException.class);
  }
}