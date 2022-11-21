package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskDiagnosticInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.util.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps tasks in cache so there is not need to query database every time.
 */
public class TasksCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(TasksCache.class);

  private final LRUCache<Long, TaskInfo> cache = new LRUCache<>(50);

  private final LRUCache<Long, TaskDiagnosticInfo> diagnosticCache = new LRUCache<>(50);

  private CassandraTaskInfoDAO taskInfoDAO;

  private TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;

  public TasksCache(CassandraConnectionProvider cassandraConnectionProvider) {
    taskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
    taskDiagnosticInfoDAO = TaskDiagnosticInfoDAO.getInstance(cassandraConnectionProvider);
  }

  public TaskInfo getTaskInfo(DpsRecord message) throws TaskInfoDoesNotExistException {
    TaskInfo taskInfo = findTaskInCache(message);
    //
    if (taskFoundInCache(taskInfo)) {
      LOGGER.trace("TaskInfo found in cache");
    } else {
      LOGGER.debug("TaskInfo NOT found in cache");
      taskInfo = readTaskFromDB(message.getTaskId());
      cache.put(message.getTaskId(), taskInfo);
    }
    return taskInfo;
  }

  private boolean taskFoundInCache(TaskInfo taskInfo) {
    return taskInfo != null;
  }

  private TaskInfo findTaskInCache(DpsRecord kafkaMessage) {
    return cache.get(kafkaMessage.getTaskId());
  }

  private TaskInfo readTaskFromDB(long taskId) throws TaskInfoDoesNotExistException {
    return findTaskInDb(taskId);
  }

  private TaskInfo findTaskInDb(long taskId) throws TaskInfoDoesNotExistException {
    return taskInfoDAO.findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new);
  }

  public TaskDiagnosticInfo getDiagnosticInfo(long taskId) {
    TaskDiagnosticInfo info = diagnosticCache.get(taskId);
    if (info == null) {
      info = taskDiagnosticInfoDAO.findById(taskId).orElse(TaskDiagnosticInfo.builder().taskId(taskId).build());
      diagnosticCache.put(taskId, info);
    }
    return info;
  }
}
