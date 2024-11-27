package eu.europeana.cloud.service.dps.controller;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskDiagnosticInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.services.postprocessors.PostProcessingService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.utils.GhostTaskService;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Scope("request")
@RequestMapping("/diag")
public class DiagnosticResource {

  private static final List<TaskState> ACTIVE_TASK_STATES = Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION,
      TaskState.QUEUED, TaskState.READY_FOR_POST_PROCESSING, TaskState.IN_POST_PROCESSING);

  @Autowired
  private GhostTaskService ghostTaskService;

  @Autowired
  private HarvestedRecordsDAO harvestedRecordsDAO;

  @Autowired
  private CassandraTaskInfoDAO taskInfoDAO;

  @Autowired
  private TasksByStateDAO tasksByStateDAO;

  @Autowired
  private PostProcessingService postProcessingService;

  @Autowired
  private TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;

  @Autowired
  private String applicationIdentifier;

  @GetMapping("/ghostTasks")
  @PreAuthorize("hasRole('ROLE_ADMIN')")
  public List<TaskInfo> ghostTasks() {
    return ghostTaskService.findGhostTasks();
  }


  @GetMapping("/harvestedRecords/{metisDatasetId}")
  @PreAuthorize("hasRole('ROLE_ADMIN')")
  public List<HarvestedRecord> harvestedRecords(
      @PathVariable("metisDatasetId") String metisDatasetId,
      @RequestParam(value = "count", defaultValue = "10") int count,
      @RequestParam(value = "oaiId", required = false) String oaiId) {
    if (oaiId != null) {
      return Collections.singletonList(harvestedRecordsDAO.findRecord(metisDatasetId, oaiId).orElse(null));
    } else {
      List<HarvestedRecord> result = new ArrayList<>();

      Iterator<HarvestedRecord> it = harvestedRecordsDAO.findDatasetRecords(metisDatasetId);
      for (var index = 0; index < count && it.hasNext(); index++) {
        HarvestedRecord theRecord = it.next();
        result.add(theRecord);
      }
      return result;
    }
  }

  @PostMapping("/postProcess/{taskId}")
  @PreAuthorize("hasRole('ROLE_ADMIN')")
  public void postProcess(@PathVariable("taskId") long taskId) {
    taskInfoDAO.findById(taskId).ifPresent(this::callPostProcess);
  }

  private void callPostProcess(TaskInfo taskInfo) {
    tasksByStateDAO.findTask(taskInfo.getState(), taskInfo.getTopologyName(), taskInfo.getId())
                   .ifPresent(taskByTaskState -> postProcessingService.postProcess(taskByTaskState));
  }


  @GetMapping(value = "/activeTasks", produces = MediaType.APPLICATION_JSON_VALUE)
  @PreAuthorize("hasRole('ROLE_ADMIN') OR hasRole('ROLE_MONITORING')")
  public String acticeTasks() throws JsonProcessingException {
    List<JoinedTaskInfo> taskInfoList = tasksByStateDAO.findTasksByState(ACTIVE_TASK_STATES).stream()
                                                       .map(TaskByTaskState::getId).map(taskInfoDAO::findById)
                                                       .flatMap(Optional::stream)
                                                       .map(this::loadExtraInfo).toList();
    return mapper().writeValueAsString(taskInfoList);
  }

  @GetMapping(value = "/task/{taskId}", produces = MediaType.APPLICATION_JSON_VALUE)
  @PreAuthorize("hasRole('ROLE_ADMIN')")
  public String task(@PathVariable("taskId") long taskId) throws JsonProcessingException {
    TaskInfo taskInfo = taskInfoDAO.findById(taskId).orElseThrow(() ->
        new ResponseStatusException(HttpStatus.NOT_FOUND, "Cant find task of id: " + taskId));
    return mapper().writeValueAsString(loadExtraInfo(taskInfo));
  }

  private JoinedTaskInfo loadExtraInfo(TaskInfo taskInfo) {
    TaskDiagnosticInfo diagnosticInfo = taskDiagnosticInfoDAO.findById(taskInfo.getId()).
                                                             orElse(
                                                                 TaskDiagnosticInfo.builder().taskId(taskInfo.getId()).build());
    TaskByTaskState tasksByState = tasksByStateDAO
        .findTask(taskInfo.getState(), taskInfo.getTopologyName(), taskInfo.getId())
        .orElse(TaskByTaskState.builder().id(taskInfo.getId()).build());
    return new JoinedTaskInfo(tasksByState, taskInfo, diagnosticInfo);
  }

  private ObjectMapper mapper() {
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.setDateFormat(isoDateFormat());
    return mapper;
  }

  private SimpleDateFormat isoDateFormat() {
    var format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return format;
  }

  @Getter
  @AllArgsConstructor
  @JsonPropertyOrder({"taskByTaskState", "info", "diagnosticInfo"})
  public static class JoinedTaskInfo {

    @JsonUnwrapped
    @JsonIgnoreProperties({"state", "topologyName", "startTime"})
    TaskByTaskState taskByTaskState;

    @JsonUnwrapped
    @JsonIgnoreProperties({"id", "definition"})
    TaskInfo info;

    @JsonUnwrapped
    @JsonIgnoreProperties({"taskId"})
    TaskDiagnosticInfo diagnosticInfo;
  }

}
