package eu.europeana.cloud.service.mcs.persistent.aspects;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataProviderDAO;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import java.util.HashMap;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/aspectTestContext.xml" })
public class ServiceExceptionTranslatorAspectTest {

    @Autowired
    private CassandraDataProviderDAO cassandraDataProviderDAO;

    @Autowired
    private CassandraDataSetDAO cassandraDataSetDAO;

    @Autowired
    private CassandraDataProviderDAO dataProviderDAO;

    @Autowired
    private RecordService cassandraRecordService;

    @Autowired
    private DataSetService dataSetService;

    @Autowired
    private DataProviderService dataProviderService;

    @Autowired
    private UISClientHandler uisHandler;
    
    
    @Test
    public void shouldTranslateExceptionInRecordService()
            throws Exception {
        // prepare failure
        Mockito.doThrow(new NoHostAvailableException(new HashMap())).when(cassandraDataProviderDAO)
                .getProvider(Mockito.anyString());
        Mockito.doReturn(true).when(uisHandler).recordExistInUIS(Mockito.anyString());
        
        // execute method to throw prepared exception and catch it
        try {
            cassandraRecordService.createRepresentation("id", "dc", "prov");
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertTrue(e.getCause() instanceof NoHostAvailableException);
        }

    }


    @Test
    public void shouldTranslateExceptionInDataSetService()
            throws Exception {
        // prepare failure
        Mockito.doThrow(new ReadTimeoutException(ConsistencyLevel.ALL, 1, 1, false)).when(cassandraDataSetDAO)
                .getDataSet(Mockito.anyString(), Mockito.anyString());

        // execute method to throw prepared exception and catch it
        try {
            dataSetService.updateDataSet("prov", "ds", "");
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertTrue(e.getCause() instanceof ReadTimeoutException);
        }

    }


    @Test
    public void shouldTranslateExceptionInDataProviderService()
            throws Exception {
        // prepare failure
        Mockito.doThrow(new ContainerNotFoundException()).when(dataProviderDAO).getProvider(Mockito.anyString());

        // execute method to throw prepared exception and catch it
        try {
            dataProviderService.getProvider("id");
        } catch (SystemException e) {
            // our wrapper should be caused by original exception
            Assert.assertTrue(e.getCause() instanceof ContainerNotFoundException);
        }
    }
}
