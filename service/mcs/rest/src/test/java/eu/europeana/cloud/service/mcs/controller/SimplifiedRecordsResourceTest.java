package eu.europeana.cloud.service.mcs.controller;

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
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

public class SimplifiedRecordsResourceTest extends AbstractResourceTest {

  @Autowired
  private SimplifiedRecordsResource recordsResource;

  @Autowired
  private RecordService recordService;

  @Autowired
  private UISClient uisClient;


  private static final String PROVIDER_ID = "providerId";
  private static final String NOT_EXISTING_PROVIDER_ID = "notExistingProviderId";
  private static final String LOCAL_ID_FOR_NOT_EXISTING_RECORD = "localIdForNotExistingRecord";
  private static final String LOCAL_ID_FOR_EXISTING_RECORD = "";
  private static final String LOCAL_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS = "localIdForRecordWithoutRepresentations";
  private static final String CLOUD_ID = "cloudId";
  private static final String CLOUD_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS = "cloudIdForRecordsWithoutRepresentations";


  @Before
  public void init() throws CloudException, RecordNotExistsException {
    setupUisClient();
    setupRecordService();
  }

  @Test(expected = ProviderNotExistsException.class)
  public void exceptionShouldBeThrowForNotExistingProviderId() throws RecordNotExistsException, ProviderNotExistsException {
    recordsResource.getRecord(null, NOT_EXISTING_PROVIDER_ID, "anyLocalId");
  }

  @Test(expected = RecordNotExistsException.class)
  public void exceptionShouldBeThrowForNotExistingCloudId() throws RecordNotExistsException, ProviderNotExistsException {
    recordsResource.getRecord(null, PROVIDER_ID, LOCAL_ID_FOR_NOT_EXISTING_RECORD);
  }

  @Test(expected = RecordNotExistsException.class)
  public void exceptionShouldBeThrowForRecordWithoutRepresentations()
      throws RecordNotExistsException, ProviderNotExistsException {
    recordsResource.getRecord(null, PROVIDER_ID, LOCAL_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS);
  }

  @Test
  public void properRecordShouldBeReturned() throws RecordNotExistsException, ProviderNotExistsException {
    HttpServletRequest info = mockHttpServletRequest();
    //Mockito.when(info.getBaseUriBuilder()).thenReturn(new JerseyUriBuilder());
    //
    Record record = recordsResource.getRecord(info, PROVIDER_ID, LOCAL_ID_FOR_EXISTING_RECORD);

    Assert.assertNotNull(record);
    for (Representation representation : record.getRepresentations()) {
      Assert.assertNull(representation.getCloudId());
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
    Mockito.doThrow(new CloudException("", new ProviderDoesNotExistException(new ErrorInfo()))).when(uisClient)
           .getCloudId(Mockito.eq(NOT_EXISTING_PROVIDER_ID), Mockito.anyString());
    Mockito.doThrow(new CloudException("", new RecordDoesNotExistException(new ErrorInfo()))).when(uisClient)
           .getCloudId(PROVIDER_ID, LOCAL_ID_FOR_NOT_EXISTING_RECORD);
    Mockito.doReturn(cid).when(uisClient).getCloudId(PROVIDER_ID, LOCAL_ID_FOR_EXISTING_RECORD);
    Mockito.doReturn(recordWithoutRepresentations).when(uisClient)
           .getCloudId(PROVIDER_ID, LOCAL_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS);
  }

  private void setupRecordService() throws RecordNotExistsException {
    Record record = new Record(CLOUD_ID, List.of(
        new Representation(CLOUD_ID, "sampleRepName", "sampleVersion", null, null, PROVIDER_ID, null, null, false, null, null, false)));
    //
    Mockito.doThrow(RecordNotExistsException.class).when(recordService).getRecord(CLOUD_ID_FOR_RECORD_WITHOUT_REPRESENTATIONS);
    Mockito.doReturn(record).when(recordService).getRecord(CLOUD_ID);
  }
}
