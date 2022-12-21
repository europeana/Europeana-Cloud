package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ServiceAndDAOTestUtils {


  public static final long TASK_ID = 111;
  public static final String TOPOLOGY_NAME = "some_topology";
  public static final String ERROR_TYPE = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b2";

  public static void createAndStoreErrorType(String errorType, CassandraTaskErrorsDAO errorsDAO) {
    errorsDAO.insertErrorCounter(TASK_ID, errorType, 5);
  }

  public static List<SubTaskInfo> createAndStoreSubtaskInfoRange(int to, NotificationsDAO notificationsDAO) {
    List<SubTaskInfo> result = new ArrayList<>();
    for (int i = 1; i <= to; i++) {
      result.add(createAndStoreNotification(i, notificationsDAO));
    }
    return result;
  }

  public static void createAndStoreErrorType(CassandraTaskErrorsDAO errorsDAO) {
    createAndStoreErrorType(ERROR_TYPE, errorsDAO);
  }

  public static void createAndStoreErrorNotification(CassandraTaskErrorsDAO errorsDAO) {
    createAndStoreErrorNotification(ERROR_TYPE, errorsDAO);
  }

  public static void createAndStoreErrorNotification(String errorType, CassandraTaskErrorsDAO errorsDAO) {
    createAndStoreErrorNotification(errorType, errorsDAO, "some_resource");
  }

  public static void createAndStoreErrorNotification(String errorType, CassandraTaskErrorsDAO errorsDAO, String resource) {
    errorsDAO.insertError(TASK_ID, errorType, "some_error_message", resource, "some_additional_information");
  }

  public static void createAndStoreTaskInfo(CassandraTaskInfoDAO taskInfoDAO) {
    TaskInfo exampleTaskInfo = new TaskInfo();
    exampleTaskInfo.setId(TASK_ID);
    exampleTaskInfo.setState(TaskState.QUEUED);
    exampleTaskInfo.setTopologyName(TOPOLOGY_NAME);
    taskInfoDAO.insert(exampleTaskInfo);
  }

  public static SubTaskInfo createAndStoreNotification(int resourceNum, NotificationsDAO subtaskInfoDao) {
    SubTaskInfo info = new SubTaskInfo(resourceNum, "resource" + resourceNum, RecordState.QUEUED, "info", "additionalInformation",
        "europeanaId", 0L, "resultResource" + resourceNum);
    subtaskInfoDao.insert(info.getResourceNum(), TASK_ID, TOPOLOGY_NAME,
        info.getResource(), info.getRecordState().toString(), info.getInfo(),
        Map.of(
            NotificationsDAO.STATE_DESCRIPTION_KEY, info.getAdditionalInformations(),
            NotificationsDAO.EUROPEANA_ID_KEY, info.getEuropeanaId(),
            NotificationsDAO.PROCESSING_TIME_KEY, String.valueOf(info.getProcessingTime())
        ),
        info.getResultResource());
    return info;
  }
}
