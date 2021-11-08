package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.helper;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.dps.storm.dao.CassandraNodeStatisticsDAO;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.when;

public class ValidationMockHelper extends TopologyTestHelper {
    protected RepresentationIterator representationIterator;
    protected CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;

    protected void mockRepresentationIterator() throws Exception {
        representationIterator = Mockito.mock(RepresentationIterator.class);
        PowerMockito.whenNew(RepresentationIterator.class).withAnyArguments().thenReturn(representationIterator);
    }

    protected void mockCassandraInteraction() throws Exception {
        super.mockCassandraInteraction();
        cassandraNodeStatisticsDAO = Mockito.mock(CassandraNodeStatisticsDAO.class);
        PowerMockito.mockStatic(CassandraNodeStatisticsDAO.class);
        when(CassandraNodeStatisticsDAO.getInstance(isA(CassandraConnectionProvider.class))).thenReturn(cassandraNodeStatisticsDAO);
    }
}
