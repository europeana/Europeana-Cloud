package eu.europeana.cloud.service.mcs.rest;


import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_REVISIONID;
import static org.junit.Assert.assertEquals;

/**
 * DataSetResourceTest
 *
 * @author akrystian
 */
@RunWith(CassandraTestRunner.class)
public class DataSetRevisionsResourceTest extends JerseyTest{

    private DataSetService dataSetService;

    private WebTarget dataSetWebTarget;

    private UISClientHandler uisHandler;

    @Override
    public Application configure(){
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }

    @Before
    public void mockUp()
            throws Exception{
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        uisHandler = applicationContext.getBean(UISClientHandler.class);
        dataSetService = applicationContext.getBean(DataSetService.class);

        dataSetWebTarget = target(DataSetRevisionsResource.class.getAnnotation(Path.class).value());
    }


    @After
    public void cleanUp() throws Exception{
        Mockito.reset(uisHandler);
    }

    @Test
    public void shouldCreateDataset() throws Exception{
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String revisionId = "revisionId";
        String representationName = "representationName";
        String cloudId = "cloudId";
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());

        dataSetService.addDataSetsRevisions(providerId, datasetId, revisionId, representationName, cloudId);

        // when
        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER,
                providerId).resolveTemplate(P_CLOUDID, cloudId).resolveTemplate(P_DATASET, datasetId)
                .resolveTemplate(P_REVISIONID, revisionId).resolveTemplate(P_REPRESENTATIONNAME, representationName);
        Response createResponse = dataSetWebTarget.request().get();

        //then
        assertEquals(Response.Status.FOUND.getStatusCode(),
                createResponse.getStatus());

    }
}