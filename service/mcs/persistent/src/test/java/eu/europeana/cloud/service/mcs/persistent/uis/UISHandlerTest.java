package eu.europeana.cloud.service.mcs.persistent.uis;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.persistent.context.UisIntegrationTestContext;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {UisIntegrationTestContext.class})
public class UISHandlerTest {

  @Autowired
  private UISClientHandlerImpl handler;

  @Autowired
  private UISClient uisClient;

  @After
  public void cleanUp() {
    Mockito.reset(uisClient);
  }

  @Test(expected = SystemException.class)
  public void shouldThrowExWhenRecordWhenUISFailure()
      throws Exception {
    String cloudId = "cloudId";
    Mockito.when(uisClient.getRecordId(cloudId)).thenThrow(
        new CloudException(cloudId, new GenericException(new IdentifierErrorInfo(
            IdentifierErrorTemplate.GENERIC_ERROR.getHttpCode(), IdentifierErrorTemplate.GENERIC_ERROR
            .getErrorInfo("")))));
    handler.existsCloudId(cloudId);
  }

  @Test
  public void shouldFailIfRecordNotFoundInUIS()
      throws Exception {
    String cloudId = "cloudId";
    Mockito.when(uisClient.getRecordId(cloudId)).thenThrow(
        new CloudException(cloudId, new CloudIdDoesNotExistException(new ErrorInfo("", ""))));
    assertFalse(handler.existsCloudId(cloudId));
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowExWhenGotNullFromUIS()
      throws Exception {
    String cloudId = "cloudId";
    Mockito.when(uisClient.getRecordId(cloudId)).thenReturn(null);
    handler.existsCloudId(cloudId);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowExWhenGotEmptyListFromUIS()
      throws Exception {
    String cloudId = "cloudId";
    Mockito.when(uisClient.getRecordId(cloudId)).thenReturn(new ResultSlice<CloudId>());
    handler.existsCloudId(cloudId);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowExWhenCloudIdNotOnListFromUIS()
      throws Exception {
    String cloudId = "cloudId";
    CloudId cl = new CloudId();
    cl.setId("66666");
    ResultSlice<CloudId> result = new ResultSlice<>();
    List<CloudId> resultList = new ArrayList<>(1);
    resultList.add(cl);
    result.setResults(resultList);

    Mockito.when(uisClient.getRecordId(cloudId)).thenReturn(result);

    handler.existsCloudId(cloudId);
  }

  @Test
  public void shouldReturnTrueWhenRecordExistsInUIS()
      throws Exception {
    String cloudId = "cloudId";
    CloudId cl = new CloudId();
    cl.setId(cloudId);
    ResultSlice<CloudId> result = new ResultSlice<>();
    List<CloudId> resultList = new ArrayList<>(1);
    resultList.add(cl);
    result.setResults(resultList);

    Mockito.when(uisClient.getRecordId(cloudId)).thenReturn(result);

    assertTrue(handler.existsCloudId(cloudId));
  }

}
