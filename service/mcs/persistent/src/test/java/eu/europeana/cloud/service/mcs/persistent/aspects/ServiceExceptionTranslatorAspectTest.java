package eu.europeana.cloud.service.mcs.persistent.aspects;

import java.util.HashMap;

import org.jclouds.blobstore.ContainerNotFoundException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import eu.europeana.cloud.common.model.DataProvider;

import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/aspectTestContext.xml" })
public class ServiceExceptionTranslatorAspectTest {

    @Autowired
    private CassandraDataSetDAO cassandraDataSetDAO;

    @Autowired
    private RecordService cassandraRecordService;

	@Autowired
	private DataSetService cassandraDataSetService;

    @Autowired
    private DataSetService dataSetService;

    @Autowired
    private UISClientHandler uis;

    @Test
    public void shouldTranslateExceptionInRecordService() throws Exception {
	// prepare failure
	Mockito.doThrow(new NoHostAvailableException(new HashMap())).when(uis)
		.getProvider("prov");

	// execute method to throw prepared exception and catch it
	try {
	    cassandraRecordService.createRepresentation("id", "dc", "prov");
	} catch (SystemException e) {
	    // our wrapper should be caused by original exception
	    Assert.assertTrue(e.getCause() instanceof NoHostAvailableException);
	}

    }

    @Test
    public void shouldTranslateExceptionInDataSetService() throws Exception {
	Mockito.doReturn(new DataProvider()).when(uis)
		.getProvider(Mockito.anyString());
	// prepare failure
	Mockito.doThrow(
		new ReadTimeoutException(ConsistencyLevel.ALL, 1, 1, false))
		.when(cassandraDataSetDAO)
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
	Mockito.doThrow(new ContainerNotFoundException()).when(uis)
		.getProvider(Mockito.anyString());

	// execute method to throw prepared exception and catch it
	try {
	    uis.getProvider("prov");
	} catch (ContainerNotFoundException e) {
	    // our wrapper should be caused by original exception
	    Assert.assertTrue(e instanceof ContainerNotFoundException);
	}
    }
}
