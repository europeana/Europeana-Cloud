package eu.europeana.cloud.service.dps.controller;

import static eu.europeana.cloud.common.log.AttributePassingUtils.TASK_ID_CONTEXT_ATTR;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.logging.AddTaskIdToLoggingContext;
import eu.europeana.cloud.service.dps.services.SubmitTaskService;
import eu.europeana.cloud.service.dps.services.validators.TaskSubmissionValidator;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

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
      @PathVariable("taskId") @AddTaskIdToLoggingContext Long taskId,
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
      @PathVariable("taskId") @AddTaskIdToLoggingContext Long taskId,
      @RequestParam(value = "info", defaultValue = "Dropped by the user") String info)
      throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
    taskSubmissionValidator.assertContainTopology(topologyName);
    reportService.checkIfTaskExists(taskId, topologyName);
    taskStatusUpdater.setTaskDropped(taskId, info);
    return ResponseEntity.ok("The task was killed because of " + info);
  }

  /**
   * Creates new task instance based on task parameters, but without starting it. Each Task is associated with a specific plugin.
   * <p/>
   * <strong>Write permissions required</strong>.
   *
   * @param task <strong>REQUIRED</strong> Task to be executed. Should contain links to input data, either in form of
   * cloud-records or cloud-datasets.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @return URI with information about the submitted task execution.
   * @throws AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the
   * @throws DpsTaskValidationException Throws if some validation error occurred
   * @throws IOException Just IOException
   */
  @PostMapping(consumes = {MediaType.APPLICATION_JSON_VALUE})
  @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
  public ResponseEntity<DpsTask> createTask(
      final HttpServletRequest request,
      @RequestBody final CreateDpsTaskRequest task,
      @PathVariable("topologyName") final String topologyName)
      throws DpsTaskValidationException, AccessDeniedOrTopologyDoesNotExistException {
    LOGGER.info("Creating task: {}", task);
    long taskId = createRandomTaskId();
    MDC.put(TASK_ID_CONTEXT_ATTR, String.valueOf(taskId));
    try {
      SubmitTaskParameters parameters = createTaskInDB(task, taskId, topologyName);
      return validateTask(request,task,taskId, parameters);
    } catch (DpsTaskValidationException | AccessDeniedOrTopologyDoesNotExistException e) {
      LOGGER.error("Error validating task: {}",task, e);
      taskStatusUpdater.updateState(taskId, EngineTaskState.INVALID, e.getMessage());
      throw e;
    } catch (Exception e) {
      return handleFailedSubmission(e, "Task submission failed. Internal server error.", taskId);
    } finally {
      MDC.remove(TASK_ID_CONTEXT_ATTR);
    }
  }

  private SubmitTaskParameters createTaskInDB(CreateDpsTaskRequest task, long taskId, String topologyName)
      throws IOException {
    var taskJSON = task.toJSON();
    SubmitTaskParameters parameters = SubmitTaskParameters.builder()
                                                          .taskInfo(
                                                              TaskInfo.builder()
                                                                      .id(taskId)
                                                                      .topologyName(topologyName)
                                                                      .state(EngineTaskState.RECEIVED)
                                                                      .stateDescription(
                                                                          "The task has been created, but not started yet")
                                                                      .sentTimestamp(new Date())
                                                                      .finishTimestamp(null)
                                                                      .expectedRecords(
                                                                          TaskInfo.UNKNOWN_EXPECTED_RECORDS_NUMBER)
                                                                      .successRecords(0)
                                                                      .warningRecords(0)
                                                                      .failRecords(0)
                                                                      .duplicateRecords(0)
                                                                      .duplicateRecords(0)
                                                                      .unchangedRecords(0)
                                                                      .processedRecords(0)
                                                                      .postProcessedRecords(0)
                                                                      .expectedPostProcessedRecords(
                                                                          TaskInfo.UNKNOWN_EXPECTED_RECORDS_NUMBER)
                                                                      .successDepublishRecords(0)
                                                                      .failDepublishRecords(0)
                                                                      .processedDepublishRecords(0)
                                                                      .definition(taskJSON)
                                                                      .build()
                                                          )
                                                          .build();

    taskStatusUpdater.insertTask(parameters);
    permissionManager.grantPermissionsForTask(String.valueOf(taskId));
    LOGGER.info("Saved received task: {} parameters in DB and added permissions.", task);
    return parameters;
  }

  private ResponseEntity<DpsTask> validateTask(HttpServletRequest request, CreateDpsTaskRequest task,
      long taskId, SubmitTaskParameters parameters)
      throws DpsTaskValidationException, AccessDeniedOrTopologyDoesNotExistException, JsonProcessingException {

    taskSubmissionValidator.validateTaskSubmission(task, parameters.getTaskInfo().getTopologyName());
    parameters.setTask(createDpsTask(task, taskId));
    parameters.getTaskInfo().setDefinition(parameters.getTask().toJSON());
    parameters.getTaskInfo().setEngineTaskState(EngineTaskState.CREATED);
    var responseURI = buildTaskURI(request, taskId);
    ResponseEntity<DpsTask> result = ResponseEntity.created(responseURI).body(parameters.getTask());
    taskStatusUpdater.insertTask(parameters);

    LOGGER.info("Created task: {}", parameters.getTask());
    return result;
  }

  private DpsTask createDpsTask(CreateDpsTaskRequest createTaskRequest, long taskId) {
    var dpsTask = new DpsTask();
    dpsTask.setSource(createTaskRequest.getSource());
    dpsTask.setResultsBatch(
        BatchInfo.builder().providerId(createTaskRequest.getResultsProvider()).batchId(String.valueOf(taskId)).build());
    dpsTask.setParameters(createTaskRequest.getParameters());
    dpsTask.setTaskId(taskId);
    return dpsTask;
  }


  /**
   * Starts the task. The method is idempotent, could be run multiple times one by one, if task is already started it has no
   * effects. However, should not be executed at the same time for the same task id, because there is no protection against
   * parallel execution of the same task.
   * <p>
   * <p/>
   * <strong>Write permissions required</strong>.
   *
   * @param taskId <strong>REQUIRED</strong> Task identifier to be processed.
   * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
   * @throws TaskInfoDoesNotExistException if topology does not exist or access to the topology is denied for the
   * @throws IOException if case of problems with task entity deserialization user
   */
  @PutMapping(path = "{taskId}/started", consumes = {MediaType.APPLICATION_JSON_VALUE})
  @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void startTask(@PathVariable("taskId") @AddTaskIdToLoggingContext final Long taskId,
      @PathVariable("topologyName") final String topologyName
  ) throws TaskInfoDoesNotExistException, IOException {

    var taskInfo = taskInfoDAO.findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new);
    if (taskInfo.getEngineTaskState().wasNotProperlyCreated()) {
      LOGGER.warn("Topology: {} task: {} could not be started because it was not created properly!",
          topologyName, taskId);
      return;
    } else if (taskInfo.getEngineTaskState() != EngineTaskState.CREATED) {
      LOGGER.info("Topology: {} task: {} is already started.", topologyName, taskId);
      return;
    }


    var dpsTask = DpsTask.fromTaskInfo(taskInfo);
    taskInfo.setStartTimestamp(new Date());
    taskInfo.setEngineTaskState(EngineTaskState.PROCESSING_BY_REST_APPLICATION);
    SubmitTaskParameters parameters = SubmitTaskParameters.builder()
                                                          .taskInfo(taskInfo)
                                                          .task(dpsTask)
                                                          .build();
    taskStatusUpdater.insertTask(parameters);
    submitTaskService.submitTask(parameters);
    LOGGER.info("Topology: {} task: {} started execution.", topologyName, taskId);
  }

  private static long createRandomTaskId() {
    return (long) (Math.random() * Long.MAX_VALUE);
  }

  private ResponseEntity<DpsTask> handleFailedSubmission(Exception exception, String loggedMessage,long taskId) {
    LOGGER.error(loggedMessage, exception);
    taskStatusUpdater.setTaskDropped(taskId, exception.getMessage());
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
  }

  private URI buildTaskURI(HttpServletRequest httpServletRequest, long taskId) {
    HttpRequest httpRequest = new ServletServerHttpRequest(httpServletRequest);
    return UriComponentsBuilder.fromUri(httpRequest.getURI()).pathSegment(String.valueOf(taskId)).build().toUri();
  }
}
