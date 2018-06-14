package eu.europeana.cloud.service.dps.storm.service.cassandra;

import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.dps.ValidationStatisticsReportService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/default-context.xml"})
public class CassandraValidationStatisticsServiceTest {

    @Autowired
    private ValidationStatisticsReportService cassandraStatisticsService;
    @Autowired
    private CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;

    private final long TASK_ID = 12345;


    @Test
    public void getTaskStatisticsReport() {
        // given
        List<NodeStatistics> stats = prepareStats();
        Mockito.when(cassandraNodeStatisticsDAO.getNodeStatistics(TASK_ID)).thenReturn(stats);
        Mockito.when(cassandraNodeStatisticsDAO.getStatisticsReport(TASK_ID)).thenReturn(null);

        // when
        StatisticsReport actual = cassandraStatisticsService.getTaskStatisticsReport(TASK_ID);

        // then
        Mockito.verify(cassandraNodeStatisticsDAO, Mockito.times(1)).storeStatisticsReport(eq(TASK_ID), Mockito.any(StatisticsReport.class));
        Mockito.verify(cassandraNodeStatisticsDAO, Mockito.times(1)).getNodeStatistics(eq(TASK_ID));

        assertEquals(TASK_ID, actual.getTaskId());
        assertThat(actual.getNodeStatistics().size(), is(2));
        assertEquals(stats, actual.getNodeStatistics());
    }

    private List<NodeStatistics> prepareStats(){
        List<NodeStatistics> nodeStatistics = new ArrayList<>();
        NodeStatistics node1 = new NodeStatistics("parentXpath", "xpath", "value", 2l);
        NodeStatistics node2 = new NodeStatistics("parentXpath2", "xpath2", "value", 2l);
        nodeStatistics.add(node1);
        nodeStatistics.add(node2);
        return nodeStatistics;
    }

    @Test
    public void getStoredTaskStatisticsReport() {
        // given
        StatisticsReport report = new StatisticsReport(TASK_ID, prepareStats());
        Mockito.when(cassandraNodeStatisticsDAO.getStatisticsReport(TASK_ID)).thenReturn(report);

        // when
        StatisticsReport actual = cassandraStatisticsService.getTaskStatisticsReport(TASK_ID);

        // then
        Mockito.verify(cassandraNodeStatisticsDAO, Mockito.times(0)).storeStatisticsReport(eq(TASK_ID), Mockito.any(StatisticsReport.class));
        Mockito.verify(cassandraNodeStatisticsDAO, Mockito.times(0)).getNodeStatistics(eq(TASK_ID));

        assertEquals(TASK_ID, actual.getTaskId());
        assertThat(actual.getNodeStatistics().size(), is(2));
        assertEquals(report, actual);
    }
}