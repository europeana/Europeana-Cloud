package eu.europeana.cloud.service.dps.storm.service.cassandra;

import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.dps.storm.CassandraTestBase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CassandraReportServiceTest extends CassandraTestBase {

    private CassandraReportService cassandraReportService;
    private final long TASK_ID = 12345;


    @Before
    public void init(){
        cassandraReportService = new CassandraReportService("localhost", 19142, "ecloud_test", "","");
    }

    @Test
    public void getTaskStatisticsReport() {
        StatisticsReport expected = new StatisticsReport(TASK_ID);
        StatisticsReport actual = cassandraReportService.getTaskStatisticsReport(Long.toString(TASK_ID));
        assertEquals(expected, actual);
    }
}