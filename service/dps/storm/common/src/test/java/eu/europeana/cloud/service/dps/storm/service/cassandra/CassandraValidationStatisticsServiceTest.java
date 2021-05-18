package eu.europeana.cloud.service.dps.storm.service.cassandra;

import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.dps.storm.utils.CassandraGeneralStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraStatisticsReportDAO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

@RunWith(PowerMockRunner.class)
public class CassandraValidationStatisticsServiceTest {

    @InjectMocks
    @Spy private CassandraValidationStatisticsService cassandraStatisticsService=new CassandraValidationStatisticsService();

    @Mock
    private CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;

    @Mock
    private CassandraGeneralStatisticsDAO cassandraGeneralStatisticsDAO;

    @Mock
    private CassandraStatisticsReportDAO cassandraStatisticsReportDAO;

    private final long TASK_ID = 12345;


    @Test
    public void getTaskStatisticsReport() {
        // given
        List<NodeStatistics> stats = prepareStats();
        doReturn(stats).when(cassandraStatisticsService).getNodeStatistics(TASK_ID);
        Mockito.when(cassandraStatisticsReportDAO.getStatisticsReport(TASK_ID)).thenReturn(null);

        // when
        StatisticsReport actual = cassandraStatisticsService.getTaskStatisticsReport(TASK_ID);

        // then
        Mockito.verify(cassandraStatisticsService, Mockito.times(1)).storeStatisticsReport(eq(TASK_ID), Mockito.any(StatisticsReport.class));
        Mockito.verify(cassandraStatisticsService, Mockito.times(1)).getNodeStatistics(eq(TASK_ID));

        assertEquals(TASK_ID, actual.getTaskId());
        assertThat(actual.getNodeStatistics().size(), is(2));
        assertEquals(stats, actual.getNodeStatistics());
    }

    private List<NodeStatistics> prepareStats(){
        List<NodeStatistics> nodeStatistics = new ArrayList<>();
        NodeStatistics node1 = new NodeStatistics("parentXpath", "xpath", "value", 2L);
        NodeStatistics node2 = new NodeStatistics("parentXpath2", "xpath2", "value", 2L);
        nodeStatistics.add(node1);
        nodeStatistics.add(node2);
        return nodeStatistics;
    }

    @Test
    public void getStoredTaskStatisticsReport() {
        // given
        StatisticsReport report = new StatisticsReport(TASK_ID, prepareStats());
        Mockito.when(cassandraStatisticsReportDAO.getStatisticsReport(TASK_ID)).thenReturn(report);

        // when
        StatisticsReport actual = cassandraStatisticsService.getTaskStatisticsReport(TASK_ID);

        // then
        Mockito.verify(cassandraStatisticsService, Mockito.times(0)).storeStatisticsReport(eq(TASK_ID), Mockito.any(StatisticsReport.class));
        Mockito.verify(cassandraStatisticsService, Mockito.times(0)).getNodeStatistics(eq(TASK_ID));

        assertEquals(TASK_ID, actual.getTaskId());
        assertThat(actual.getNodeStatistics().size(), is(2));
        assertEquals(report, actual);
    }
}