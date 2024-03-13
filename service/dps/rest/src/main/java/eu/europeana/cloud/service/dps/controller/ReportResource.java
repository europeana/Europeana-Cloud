package eu.europeana.cloud.service.dps.controller;

import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.ValidationStatisticsService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.parameters.P;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Scope("request")
@RequestMapping("/{topologyName}/tasks")
public class ReportResource {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(ReportResource.class);

  public static final String TASK_PREFIX = "DPS_Task";

  @Value("${maxIdentifiersCount}")
  private int maxIdentifiersCount;

  @Autowired
  private TopologyManager topologyManager;

  @Autowired
  private TaskExecutionReportService reportService;

  @Autowired
  private ValidationStatisticsService validationStatisticsService;

  /**
   * Retrieve task detailed report Retrieves a detailed report for the specified task.It will return info about the first 100
   * resources unless you specified the needed chunk by using from&to parameters
   * <p/>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   * <strong>Required permissions:</strong>
   * <ul>
   * <li>Authenticated user</li>
   * <li>Read permission for selected task</li>
   * </ul>
   * </div>
   *
   * @param taskId <strong>REQUIRED</strong> Unique id that identifies the task.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @param from The starting resource number should be bigger than 0
   * @param to The ending resource number should be bigger than 0
   * @return Notification messages for the specified task.
   */
  @GetMapping(path = "{taskId}/reports/details", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
  @PreAuthorize("hasPermission(#taskId.toString(),'" + TASK_PREFIX + "', read)")
  public List<SubTaskInfo> getTaskDetailedReport(
      @PathVariable("taskId") @P("taskId") Long taskId,
      @PathVariable("topologyName") final String topologyName,
      @RequestParam(value = "from", defaultValue = "1") @Min(1) int from,
      @RequestParam(value = "to", defaultValue = "100") @Min(1) int to)
      throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
    assertContainTopology(topologyName);
    reportService.checkIfTaskExists(taskId, topologyName);

    return reportService.getDetailedTaskReport(taskId, from, to);
  }


  /**
   * If error param is not specified it retrieves a report of all errors that occurred for the specified task. For each error the
   * number of occurrences is returned otherwise retrieves a report for a specific error that occurred in the specified task. A
   * sample of identifiers is returned as well. The number of identifiers is between 0 and ${maxIdentifiersCount}.
   * <p>
   * <p/>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   * <strong>Required permissions:</strong>
   * <ul>
   * <li>Authenticated user</li>
   * <li>Read permission for selected task</li>
   * </ul>
   * </div>
   *
   * @param taskId <strong>REQUIRED</strong> Unique id that identifies the task.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @param error Error type.
   * @param idsCount number of identifiers to retrieve
   * @return Errors that occurred for the specified task.
   */
  @GetMapping(path = "{taskId}/reports/errors", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
  @PreAuthorize("hasPermission(#taskId.toString(),'" + TASK_PREFIX + "', read)")
  public TaskErrorsInfo getTaskErrorReport(
      @PathVariable("taskId") @P("taskId") Long taskId,
      @PathVariable("topologyName") final String topologyName,
      @RequestParam(value = "error", required = false) String error,
      @RequestParam(value = "idsCount", defaultValue = "0") int idsCount)
      throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
    assertContainTopology(topologyName);
    reportService.checkIfTaskExists(taskId, topologyName);

    if (idsCount < 0 || idsCount > maxIdentifiersCount) {
      throw new IllegalArgumentException("Identifiers count parameter should be between 0 and " + maxIdentifiersCount);
    }
    if (error == null || error.equals("null")) {
      return reportService.getGeneralTaskErrorReport(taskId, idsCount);
    }
    return reportService.getSpecificTaskErrorReport(taskId, error, idsCount > 0 ? idsCount : maxIdentifiersCount);
  }


  /**
   * Check if the task has error report
   * <p>
   * <p/>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   * <strong>Required permissions:</strong>
   * <ul>
   * <li>Authenticated user</li>
   * <li>Read permission for selected task</li>
   * </ul>
   * </div>
   *
   * @param taskId <strong>REQUIRED</strong> Unique id that identifies the task.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @return if the error report exists
   */
  @RequestMapping(method = {RequestMethod.HEAD}, path = "{taskId}/reports/errors")
  @PreAuthorize("hasPermission(#taskId.toString(),'" + TASK_PREFIX + "', read)")
  public ResponseEntity checkIfErrorReportExists(
      @PathVariable("taskId") @P("taskId") Long taskId,
      @PathVariable("topologyName") final String topologyName)
      throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {

    assertContainTopology(topologyName);
    reportService.checkIfTaskExists(taskId, topologyName);
    return (reportService.checkIfReportExists(taskId) ? ResponseEntity.ok()
        : ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED)).build();
  }


  /**
   * Retrieves a statistics report for the specified task. Only applicable for tasks executing {link
   * eu.europeana.cloud.service.dps.storm.topologies.validation.topology.ValidationTopology}
   * <p>
   * <p/>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   * <strong>Required permissions:</strong>
   * <ul>
   * <li>Authenticated user</li>
   * <li>Read permission for selected task</li>
   * </ul>
   * </div>
   *
   * @param taskId <strong>REQUIRED</strong> Unique id that identifies the task.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @return Statistics report for the specified task.
   */
  @GetMapping(path = "{taskId}/statistics", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
  @PreAuthorize("hasPermission(#taskId.toString(),'" + TASK_PREFIX + "', read)")
  public StatisticsReport getTaskStatisticsReport(
      @PathVariable("topologyName") String topologyName,
      @PathVariable("taskId") @P("taskId") Long taskId)
      throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
    assertContainTopology(topologyName);
    reportService.checkIfTaskExists(taskId, topologyName);
    return validationStatisticsService.getTaskStatisticsReport(taskId);
  }


  /**
   * Retrieves a list of distinct values and their occurrences for a specific element based on its path}
   * <p>
   * <p/>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   * <strong>Required permissions:</strong>
   * <ul>
   * <li>Authenticated user</li>
   * <li>Read permission for selected task</li>
   * </ul>
   * </div>
   *
   * @param taskId <strong>REQUIRED</strong> Unique id that identifies the task.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @param elementPath <strong>REQUIRED</strong> Path for specific element.
   * @return List of distinct values and their occurrences.
   */
  @GetMapping(path = "{taskId}/reports/element", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
  @PreAuthorize("hasPermission(#taskId.toString(),'" + TASK_PREFIX + "', read)")
  public List<NodeReport> getElementsValues(
      @PathVariable("topologyName") String topologyName,
      @PathVariable("taskId") @P("taskId") Long taskId,
      @NotNull @RequestParam("path") String elementPath)
      throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
    assertContainTopology(topologyName);
    reportService.checkIfTaskExists(taskId, topologyName);
    return validationStatisticsService.getElementReport(taskId, elementPath);
  }

  private void assertContainTopology(String topology) throws AccessDeniedOrTopologyDoesNotExistException {
    if (!topologyManager.containsTopology(topology)) {
      throw new AccessDeniedOrTopologyDoesNotExistException("The topology doesn't exist");
    }
  }
}
