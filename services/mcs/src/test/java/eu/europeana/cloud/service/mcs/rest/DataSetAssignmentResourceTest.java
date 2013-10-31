package eu.europeana.cloud.service.mcs.rest;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_DATASET;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_PROVIDER;

/**
 * DataSetAssignmentResourceTest
 */
public class DataSetAssignmentResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private DataSetService dataSetService;

    private RecordService recordService;

    private WebTarget dataSetAssignmentWebTarget;

    private DataProvider dataProvider;

    private DataSet dataSet;


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderService = applicationContext.getBean(DataProviderService.class);
        dataSetService = applicationContext.getBean(DataSetService.class);
        recordService = applicationContext.getBean(RecordService.class);
        dataSetAssignmentWebTarget = target("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}/assignments");
        dataProvider = dataProviderService.createProvider("provident", new DataProviderProperties());
        dataSetService.createDataSet(dataProvider.getId(), "dataset", "description");
    }


    @After
    public void cleanUp() {
        for (DataProvider prov : dataProviderService.getProviders()) {
            for (DataSet ds : dataSetService.getDataSets(prov.getId())) {
                dataSetService.deleteDataSet(prov.getId(), ds.getId());
            }
            dataProviderService.deleteProvider(prov.getId());
        }
    }


    @Test
    @Ignore(value = "TODO: implement")
    public void shouldAddAssignmentForLatestVersion() {
    }


    @Test
    @Ignore(value = "TODO: implement")
    public void shouldAddAssignmentForSpecificVersion() {
    }


    @Test
    @Ignore(value = "TODO: implement")
    public void shouldRemoveAssignment() {
    }
}
