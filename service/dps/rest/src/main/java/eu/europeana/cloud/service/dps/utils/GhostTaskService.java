package eu.europeana.cloud.service.dps.utils;


import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskDiagnosticInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.properties.KafkaProperties;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class GhostTaskService {

  private static final Logger LOGGER = LoggerFactory.getLogger(GhostTaskService.class);

  private TasksByStateDAO tasksByStateDAO;

  private CassandraTaskInfoDAO taskInfoDAO;

  private TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;


  private final Set<String> availableTopic;

  @Autowired
  public GhostTaskService(TasksByStateDAO tasksByStateDAO,
      CassandraTaskInfoDAO cassandraTaskInfoDAO,
      TaskDiagnosticInfoDAO taskDiagnosticInfo,
      KafkaProperties kafkaProperties) {
    this.tasksByStateDAO = tasksByStateDAO;
    this.taskInfoDAO = cassandraTaskInfoDAO;
    this.taskDiagnosticInfoDAO = taskDiagnosticInfo;
    availableTopic = new TopologiesTopicsParser().parse(kafkaProperties.getTopologyAvailableTopics())
                                                 .values().stream().flatMap(List::stream).collect(Collectors.toSet());
  }

  @Scheduled(cron = "0 0 * * * *")
  public void serviceTask() {
    List<TaskInfo> tasks = findGhostTasks();
    List<Long> ids = tasks.stream().map(TaskInfo::getId).collect(Collectors.toList());
    if (!ids.isEmpty()) {
      LOGGER.error("Ghost task found on server ids: {}", ids);
    } else {
      LOGGER.info("Ghost task on server not found");
    }
  }

  public List<TaskInfo> findGhostTasks() {
    return findTasksInGivenStates(TaskState.PROCESSING_BY_REST_APPLICATION, TaskState.QUEUED).
        filter(this::isGhost).collect(Collectors.toList());
  }

  private Stream<TaskInfo> findTasksInGivenStates(TaskState... states) {
    return tasksByStateDAO.findTasksByState(Arrays.asList(states)).stream()
                          .filter(info -> availableTopic.contains(info.getTopicName())).map(TaskByTaskState::getId)
                          .map(taskInfoDAO::findById).flatMap(Optional::stream);
  }

  private boolean isGhost(TaskInfo task) {
    return taskDidNotProgressOnStormRecently(task) || taskIsVeryOld(task);
  }

  private boolean taskDidNotProgressOnStormRecently(TaskInfo task) {
    return isDateTooOld(task.getSentTimestamp().toInstant())
        && taskDiagnosticInfoDAO.findById(task.getId())
                                .map(TaskDiagnosticInfo::getLastRecordFinishedOnStormTime)
                                .map(this::isDateTooOld)
                                .orElse(true);
  }

  private boolean taskIsVeryOld(TaskInfo task) {
    //Value 60 days is related with Cassandra TTL period for tables notifications and processed_records
    //If task is older this data could disappear, and task could not finish forever.
    return task.getSentTimestamp().toInstant().isBefore(Instant.now().minus(60, ChronoUnit.DAYS));
  }

  private boolean isDateTooOld(Instant date) {
    return date.isBefore(Instant.now().minus(2, ChronoUnit.DAYS));
  }

}
