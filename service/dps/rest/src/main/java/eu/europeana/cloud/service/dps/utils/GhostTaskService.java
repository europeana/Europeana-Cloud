package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static eu.europeana.cloud.service.dps.config.JndiNames.JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS;

@Service
public class GhostTaskService {

    private static final Logger LOGGER = LoggerFactory.getLogger(GhostTaskService.class);

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    private final Set<String> availableTopic;

    public GhostTaskService(Environment environment) {
        availableTopic = new TopologiesTopicsParser().parse(environment.getProperty(JNDI_KEY_TOPOLOGY_AVAILABLE_TOPICS))
                .values().stream().flatMap(List::stream).collect(Collectors.toSet());
    }

    @Scheduled(cron = "0 0 * * * *")
    public void serviceTask() {
        List<TaskInfo> tasks = findGhostTasks();
        List<Long> ids = tasks.stream().map(TaskInfo::getId).collect(Collectors.toList());
        LOGGER.error("Ghost task found on server ids: {}", ids);
    }

    public List<TaskInfo> findGhostTasks() {
        return findTasksInGivenStates(TaskState.PROCESSING_BY_REST_APPLICATION, TaskState.QUEUED).
                filter(this::isGhost).collect(Collectors.toList());
    }

    private Stream<TaskInfo> findTasksInGivenStates(TaskState... states) {
        return Arrays.stream(states).map(tasksByStateDAO::findTasksInGivenState)
                .flatMap(List::stream)
                .filter(info -> availableTopic.contains(info.getTopicName())).map(TaskInfo::getId)
                .map(taskInfoDAO::findById).flatMap(Optional::stream);
    }

    private boolean isGhost(TaskInfo task) {
        return isDateTooOld(task.getSentDate()) && ((task.getStartDate() == null) || isDateTooOld(task.getStartDate()));
    }

    private boolean isDateTooOld(Date date) {
        return date.toInstant().isBefore(Instant.now().minus(10, ChronoUnit.DAYS));
    }
}
