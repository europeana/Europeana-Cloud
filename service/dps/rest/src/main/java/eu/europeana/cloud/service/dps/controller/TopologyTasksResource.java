package eu.europeana.cloud.service.dps.controller;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.services.SubmitTaskService;
import eu.europeana.cloud.service.dps.services.validators.TaskSubmissionValidator;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Resource to fetch / submit Tasks to the DPS service
 */
@RestController
@RequestMapping("/{topologyName}/tasks")
public class TopologyTasksResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyTasksResource.class);

  private static final String TOPOLOGY_PREFIX = "Topology";

  public static final String TASK_PREFIX = "DPS_Task";

  @Autowired
  private TaskExecutionReportService reportService;

  @Autowired
  private PermissionManager permissionManager;

  @Autowired
  private CassandraTaskInfoDAO taskInfoDAO;

  @Autowired
  private TaskStatusUpdater taskStatusUpdater;

  @Autowired
  private SubmitTaskService submitTaskService;

  @Autowired
  private TaskSubmissionValidator taskSubmissionValidator;

  /**
   * Retrieves the current progress for the requested task.
   * <p/>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   * <strong>Required permissions:</strong>
   * <ul>
   * <li>Read permissions for selected task</li>
   * </ul>
   * </div>
   *
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @param taskId <strong>REQUIRED</strong> Unique id that identifies the task.
   * @return Progress for the requested task (number of records of the specified task that have been fully processed).
   * @throws AccessDeniedOrObjectDoesNotExistException if task does not exist or access to the task is denied for the user
   * @throws AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the
   * user
   */

  @GetMapping(value = "{taskId}/progress", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
  @PreAuthorize("hasPermission(#taskId.toString(),'" + TASK_PREFIX + "', read)")
  public TaskInfo getTaskProgress(
      @PathVariable("topologyName") final String topologyName,
      @PathVariable("taskId") final Long taskId) throws
      AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {
    LOGGER.info("Checking task progress for: {}", taskId);
    taskSubmissionValidator.assertContainTopology(topologyName);
    reportService.checkIfTaskExists(taskId, topologyName);
    TaskInfo taskProgress = reportService.getTaskProgress(taskId);
    LOGGER.info("Following task progress will be sent to user: {}", taskProgress);
    return taskProgress;
  }

  /**
   * Submits a Task for execution. Each Task execution is associated with a specific plugin.
   * <p/>
   * <strong>Write permissions required</strong>.
   *
   * @param task <strong>REQUIRED</strong> Task to be executed. Should contain links to input data, either in form of
   * cloud-records or cloud-datasets.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @return URI with information about the submitted task execution.
   * @throws AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the
   * user
   */
  @PostMapping(consumes = {MediaType.APPLICATION_JSON_VALUE})
  @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
  public ResponseEntity<Void> submitTask(
      final HttpServletRequest request,
      @RequestBody final DpsTask task,
      @PathVariable("topologyName") final String topologyName
  ) throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
    return doSubmitTask(request, task, topologyName, false);
  }

  /**
   * Restarts a Task for execution. Each Task execution is associated with a specific plugin.
   * <p/>
   * <strong>Write permissions required</strong>.
   *
   * @param taskId <strong>REQUIRED</strong> Task identifier to be processed.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @return URI with information about the submitted task execution.
   * @throws AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the
   * user
   */
  @PostMapping(path = "{taskId}/restart", consumes = {MediaType.APPLICATION_JSON_VALUE})
  @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
  public ResponseEntity<Void> restartTask(
      final HttpServletRequest request,
      @PathVariable("taskId") final Long taskId,
      @PathVariable("topologyName") final String topologyName
  ) throws TaskInfoDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
    var taskInfo = taskInfoDAO.findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new);
    var task = DpsTask.fromTaskInfo(taskInfo);
    return doSubmitTask(request, task, topologyName, true);
  }

  /**
   * Grants read / write permissions for a task to the specified user.
   * <p/>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   * <strong>Required permissions:</strong>
   * <ul>
   * <li>Admin permissions</li>
   * </ul>
   * </div>
   *
   * @param taskId <strong>REQUIRED</strong> Unique id that identifies the task.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @param username <strong>REQUIRED</strong> Permissions are granted to the account with this unique username
   * @return Status code indicating whether the operation was successful or not.
   * @throws AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the
   * user
   */

  @PostMapping(path = "{taskId}/permit")
  @PreAuthorize("hasRole('ROLE_ADMIN')")
  public ResponseEntity<Void> grantPermissions(
      @PathVariable("topologyName") String topologyName,
      @PathVariable("taskId") Long taskId,
      @RequestParam("username") String username) throws AccessDeniedOrTopologyDoesNotExistException {

    taskSubmissionValidator.assertContainTopology(topologyName);

    if (taskId != null) {
      permissionManager.grantPermissionsForTask(String.valueOf(taskId), username);
      return ResponseEntity.ok().build();
    }
    return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
  }

  /**
   * Submit kill flag to the specific task.
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   * <strong>Required permissions:</strong>
   * <ul>
   * <li>Authenticated user</li>
   * <li>Write permission for selected task</li>
   * </ul>
   * </div>
   *
   * @param taskId <strong>REQUIRED</strong> Unique id that identifies the task.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @param info <strong>OPTIONAL</strong> The cause of the cancellation. If it was not specified a default cause 'Dropped by the
   * user' will be provided
   * @return Status code indicating whether the operation was successful or not.
   * @throws AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the
   * user
   * @throws AccessDeniedOrObjectDoesNotExistException if taskId does not belong to the specified topology
   */

  @PostMapping(path = "{taskId}/kill")
  @PreAuthorize("hasRole('ROLE_ADMIN') OR  hasPermission(#taskId.toString(),'" + TASK_PREFIX + "', write)")
  public ResponseEntity<String> killTask(
      @PathVariable("topologyName") String topologyName,
      @PathVariable("taskId") Long taskId,
      @RequestParam(value = "info", defaultValue = "Dropped by the user") String info)
      throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
    taskSubmissionValidator.assertContainTopology(topologyName);
    reportService.checkIfTaskExists(taskId, topologyName);
    taskStatusUpdater.setTaskDropped(taskId, info);
    return ResponseEntity.ok("The task was killed because of " + info);
  }

  /**
   * Common method for submit/restart task. Mode is given in restart parameter
   *
   * @param task Task to process to
   * @param topologyName Name of processing topology
   * @param restart Mode (submit = <code>false</code> / restart = <code>true</code>) flag
   * @return Respons for rest call
   * @throws AccessDeniedOrTopologyDoesNotExistException Throws if access is denied or topology does not exist
   * @throws DpsTaskValidationException Throws if some validation error occurred
   * @throws IOException Just IOException
   */
  private ResponseEntity<Void> doSubmitTask(
      final HttpServletRequest request,
      final DpsTask task, final String topologyName,
      final boolean restart)
      throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {

    ResponseEntity<Void> result;

    if (task != null) {
      LOGGER.info(!restart ? "Submitting task: {}" : "Restarting task: {}", task);

      Date sentTime = new Date();

      var taskJSON = task.toJSON();
      SubmitTaskParameters parameters = SubmitTaskParameters.builder()
                                                            .taskInfo(
                                                                TaskInfo.builder()
                                                                        .id(task.getTaskId())
                                                                        .topologyName(topologyName)
                                                                        .state(TaskState.PROCESSING_BY_REST_APPLICATION)
                                                                        .stateDescription(
                                                                            "The task is in a pending mode, it is being processed before submission")
                                                                        .sentTimestamp(sentTime)
                                                                        .startTimestamp(new Date())
                                                                        .finishTimestamp(null)
                                                                        .expectedRecordsNumber(0)
                                                                        .processedRecordsCount(0)
                                                                        .ignoredRecordsCount(0)
                                                                        .deletedRecordsCount(0)
                                                                        .processedErrorsCount(0)
                                                                        .deletedErrorsCount(0)
                                                                        .expectedPostProcessedRecordsNumber(-1)
                                                                        .postProcessedRecordsCount(0)
                                                                        .definition(taskJSON)
                                                                        .build()
                                                            )
                                                            .task(task)
                                                            .restarted(restart).build();
      try {
        taskStatusUpdater.insertTask(parameters);
        taskSubmissionValidator.validateTaskSubmission(parameters);
        permissionManager.grantPermissionsForTask(String.valueOf(task.getTaskId()));
        submitTaskService.submitTask(parameters);
        var responseURI = buildTaskURI(request, task);
        result = ResponseEntity.created(responseURI).build();
      } catch (DpsTaskValidationException | AccessDeniedOrTopologyDoesNotExistException e) {
        taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), e.getMessage());
        throw e;
      } catch (Exception e) {
        result = handleFailedSubmission(e, "Task submission failed. Internal server error.", parameters);
      }
    } else {
      LOGGER.error("Task submission failed. Internal server error. DpsTask task is null.");
      result = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

    return result;
  }

  private ResponseEntity<Void> handleFailedSubmission(Exception exception, String loggedMessage,
      SubmitTaskParameters parameters) {
    LOGGER.error(loggedMessage);
    taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), exception.getMessage());
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
  }

  private URI buildTaskURI(HttpServletRequest httpServletRequest, DpsTask task) {
    HttpRequest httpRequest = new ServletServerHttpRequest(httpServletRequest);
    return UriComponentsBuilder.fromHttpRequest(httpRequest).pathSegment(String.valueOf(task.getTaskId())).build().toUri();
  }
}
