package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import eu.europeana.cloud.service.uis.exception.GenericException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import java.util.ArrayList;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/uisIntegrationTestContext.xml"})
public class UISHandlerTest {

    @Autowired
    private UISClientHandler handler;

    @Autowired
    private UISClient uisClient;

    @Test(expected = SystemException.class)
    public void shouldThrowExWhenRecordWhenUISFailure() throws Exception {
        String cloudId = "cloudId";
        Mockito.reset(uisClient);
        Mockito.when(uisClient.getRecordId(cloudId)).thenThrow(new CloudException(cloudId, new GenericException(cloudId)));
        handler.recordExistInUIS(cloudId);
    }

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowExWhenRecordNotFoundInUIS() throws Exception {
        String cloudId = "cloudId";
        Mockito.reset(uisClient);
        Mockito.when(uisClient.getRecordId(cloudId)).thenThrow(new CloudException(cloudId, new RecordDoesNotExistException(cloudId)));
        handler.recordExistInUIS(cloudId);
    }

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowExWhenGotNullFromUIS() throws Exception {
        String cloudId = "cloudId";
        Mockito.reset(uisClient);
        Mockito.when(uisClient.getRecordId(cloudId)).thenReturn(null);
        handler.recordExistInUIS(cloudId);
    }

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowExWhenGotEmptyListFromUIS() throws Exception {
        String cloudId = "cloudId";
        Mockito.reset(uisClient);
        Mockito.when(uisClient.getRecordId(cloudId)).thenReturn(new ArrayList<CloudId>());
        handler.recordExistInUIS(cloudId);
    }

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowExWhenCloudIdNotOnListFromUIS() throws Exception {
        String cloudId = "cloudId";
        CloudId cl = new CloudId();
        cl.setId("66666");
        ArrayList<CloudId> result = new ArrayList<>();
        result.add(cl);

        Mockito.reset(uisClient);
        Mockito.when(uisClient.getRecordId(cloudId)).thenReturn(result);

        handler.recordExistInUIS(cloudId);
    }

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowExWhenRecordWhenUISReturnedEmptyList() throws Exception {
        String cloudId = "cloudId";
        Mockito.reset(uisClient);
        Mockito.when(uisClient.getRecordId(cloudId)).thenReturn(new ArrayList<CloudId>());
        handler.recordExistInUIS(cloudId);
    }

    @Test
    public void shouldReturnTrueWhenRecordExistsInUIS() throws Exception {
        String cloudId = "cloudId";
        CloudId cl = new CloudId();
        cl.setId(cloudId);
        ArrayList<CloudId> result = new ArrayList<>();
        result.add(cl);

        Mockito.reset(uisClient);
        Mockito.when(uisClient.getRecordId(cloudId)).thenReturn(result);

        assertTrue(handler.recordExistInUIS(cloudId));
    }

}
