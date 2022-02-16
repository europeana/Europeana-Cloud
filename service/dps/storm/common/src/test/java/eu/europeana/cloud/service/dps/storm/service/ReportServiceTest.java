package eu.europeana.cloud.service.dps.storm.service;

import com.google.common.collect.Lists;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;


public class ReportServiceTest extends CassandraTestBase {

    private static final long TASK_ID_LONG = 111;
    private static final String TASK_ID = String.valueOf(TASK_ID_LONG);
    private static final String TOPOLOGY_NAME = "some_topology";
    private ReportService service;
    private NotificationsDAO subtaskInfoDao;

    @Before
    public void setup() {
        CassandraConnectionProvider db = new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER, PASSWORD);
        service = new ReportService(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER, PASSWORD);
        subtaskInfoDao = NotificationsDAO.getInstance(db);
    }

    @Test
    public void shouldReturnEmptyReportWhenThereIsNoDataWhenQueryingOneRecord() {
        List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, 1);

        assertThat(report, hasSize(0));
    }

    @Test
    public void shouldReturnEmptyReportWhenThereIsNoDataWhenQueryingOneBucket() {
        List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, 100);

        assertThat(report, hasSize(0));
    }

    @Test
    public void shouldReturnEmptyReportWhenThereIsNoDataWhenQueryingManyBuckets() {
        List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, NotificationsDAO.BUCKET_SIZE * 3);

        assertThat(report, hasSize(0));
    }

    @Test
    public void shouldReturnValidReportWhenQueryingOneRecord() {
        SubTaskInfo info = createAndStoreSubtaskInfo(1);

        List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, 1);

        assertThat(report, hasSize(1));
        assertEquals(info, report.get(0));
    }

    @Test
    public void shouldReturnValidReportWhenQueryingOneBucket() {
        shouldReturnValidReportWhenQueryingBucketTemplate(100, 100);
    }

    @Test
    public void shouldReturnValidReportWhenQueryingOneBucketAtBorderOfData() {
        shouldReturnValidReportWhenQueryingBucketTemplate(100, 200);
    }

    @Test
    public void shouldReturnValidReportWhenQueryingManyBuckets() {
        shouldReturnValidReportWhenQueryingBucketTemplate(30000, 30000);
    }

    @Test
    public void shouldReturnValidReportWhenQueryingDataOverlappingBuckets() {
        shouldReturnValidReportWhenQueryingBucketTemplate(25000, 50000);
    }

    @Test
    public void shouldReturnValidReportWhenQueryingPartOfDataInOneBucketAtBorderOfData() {
        List<SubTaskInfo> infoList =
                createAndStoreSubtaskInfoRange(300).subList(100 - 1, 200);

        List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 100, 200);

        assertEquals(Lists.reverse(infoList), report);
    }


    @Test
    public void shouldReturnValidReportWhenQueryingPartOfDataInFurtherBucket() {
        List<SubTaskInfo> infoList =
                createAndStoreSubtaskInfoRange(40000).subList(21000 - 1, 22000);

        List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 21000, 22000);

        assertEquals(Lists.reverse(infoList), report);
    }

    private void shouldReturnValidReportWhenQueryingBucketTemplate(int preparedReportSize, int requestTo) {
        List<SubTaskInfo> infoList = createAndStoreSubtaskInfoRange(preparedReportSize);

        List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, requestTo);

        assertEquals(Lists.reverse(infoList), report);
    }

    private List<SubTaskInfo> createAndStoreSubtaskInfoRange(int to) {
        List<SubTaskInfo> result = new ArrayList<>();
        for (int i = 1; i <= to; i++) {
            result.add(createAndStoreSubtaskInfo(i));
        }
        return result;
    }

    private SubTaskInfo createAndStoreSubtaskInfo(int resourceNum) {
        SubTaskInfo info = new SubTaskInfo(resourceNum, "resource" + resourceNum, RecordState.QUEUED, "info", "additionalInformation",  "recordId","resultResource" + resourceNum);
        subtaskInfoDao.insert(info.getResourceNum(), TASK_ID_LONG, TOPOLOGY_NAME,
                info.getResource(), info.getRecordState().toString(), info.getInfo(),
                Map.of(NotificationsDAO.ADDITIONAL_INFO_TEXT_KEY, info.getAdditionalInformations(), NotificationsDAO.ADDITIONAL_INFO_RECORD_ID_KEY, info.getRecordId()),
                info.getResultResource());
        return info;
    }

}