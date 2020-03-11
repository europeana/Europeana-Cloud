package eu.europeana.cloud.service.dps.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.converters.DpsTaskToHarvestConverter;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaningException;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.dps.utils.DpsTaskValidatorFactory;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.ws.Holder;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import static eu.europeana.cloud.service.dps.InputDataType.*;

/**
 * Resource to fetch / submit Tasks to the DPS service
 */
@RestController
@Scope("request")
@RequestMapping("/{topologyName}/tasks")
@Validated
public class TopologyTasksResource {
    @Value("${maxIdentifiersCount}")
    private int maxIdentifiersCount;

    @Autowired
    private ApplicationContext appCtx;

    @Autowired
    private TaskExecutor taskExecutor;

    @Autowired
    private HttpServletRequest request;

    @Autowired
    private TaskExecutionReportService reportService;

    @Autowired
    private ValidationStatisticsReportService validationStatisticsService;

    @Autowired
    private TaskExecutionKillService killService;

    @Autowired
    private TopologyManager topologyManager;

    @Autowired
    private PermissionManager permissionManager;

    @Autowired
    private DataSetServiceClient dataSetServiceClient;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    private final static String TOPOLOGY_PREFIX = "Topology";

    public final static String TASK_PREFIX = "DPS_Task";

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyTasksResource.class);

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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @return Progress for the requested task
     * (number of records of the specified task that have been fully processed).
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException   if task does not exist or access to the task is denied for the user
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Get Task Progress
     */

    @GetMapping(value = "{taskId}/progress", produces = {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public TaskInfo getTaskProgress(
            @PathVariable final String topologyName,
            @PathVariable final String taskId) throws
                            AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        TaskInfo progress = reportService.getTaskProgress(taskId);
        return progress;
    }

    /**
     * Submits a Task for execution.
     * Each Task execution is associated with a specific plugin.
     * <p/>
     * <strong>Write permissions required</strong>.
     *
     * @param task         <strong>REQUIRED</strong> Task to be executed. Should contain links to input data,
     *                     either in form of cloud-records or cloud-datasets.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @return URI with information about the submitted task execution.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Submit Task
     * @summary Submit Task
     */
    @PostMapping(consumes = {MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    @Async
    public DeferredResult<Response> submitTask(
            @RequestBody final DpsTask task,
            @PathVariable final String topologyName,
            @RequestHeader("Authorization") final String authorizationHeader
    ) throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException, ExecutionException, InterruptedException {
        return doSubmitTask(task, topologyName, authorizationHeader, false);
    }

    public DeferredResult<Response> submitTask(
            final DpsTask task,
            final String topologyName,
            final UriInfo uriInfo,
            final String authorizationHeader
    ) throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException, ExecutionException, InterruptedException {
        return null;
    }


    /**
     * Restarts a Task for execution.
     * Each Task execution is associated with a specific plugin.
     * <p/>
     * <strong>Write permissions required</strong>.
     *
     * @param taskId       <strong>REQUIRED</strong> Task identifier to be processed.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @return URI with information about the submitted task execution.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Submit Task
     * @summary Submit Task
     */
    @PostMapping(path = "{taskId}/restart", consumes = {MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    @Async
    public DeferredResult<Response> restartTask(
            @PathVariable final long taskId,
            @PathVariable final String topologyName,
            @RequestHeader("Authorization") final String authorizationHeader
    ) throws TaskInfoDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException, ExecutionException, InterruptedException {
        TaskInfo taskInfo = taskInfoDAO.searchById(taskId);
        DpsTask task = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
        return doSubmitTask(task, topologyName, authorizationHeader, true);
    }

    /**
     *
     * @param task
     * @param topologyName
     * @param authorizationHeader
     * @param restart
     * @return
     * @throws AccessDeniedOrTopologyDoesNotExistException
     * @throws DpsTaskValidationException
     * @throws IOException
     */
    private DeferredResult<Response> doSubmitTask(
            final DpsTask task, final String topologyName,
            final String authorizationHeader, final boolean restart)
            throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {

        final long SUBMIT_TASK_TIMEOUT = 5*60*1000;  //5min

        DeferredResult result = new DeferredResult(SUBMIT_TASK_TIMEOUT);

        if (task != null) {
            LOGGER.info(!restart ? "Submitting task" : "Restarting task");
            assertContainTopology(topologyName);
            validateTask(task, topologyName);
            validateOutputDataSetsIfExist(task);
            final Date sentTime = new Date();
            task.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, authorizationHeader);
            final String taskJSON = new ObjectMapper().writeValueAsString(task);
            TaskStatusChecker.init(taskInfoDAO);

            URI responsURI = null;
            try {
                responsURI = buildTaskURI(request.getRequestURL(), task);
            } catch (URISyntaxException e) {
                LOGGER.error("Task submission failed");
                ResponseEntity response = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
                taskInfoDAO.insert(task.getTaskId(), topologyName, 0,
                        TaskState.DROPPED.toString(), e.getMessage(), sentTime, taskJSON);
                result.setResult(response);
            }

            SubmitTaskParameters parameters = SubmitTaskParameters.builder()
                    .task(task)
                    .topologyName(topologyName)
                    .authorizationHeader(authorizationHeader)
                    .restart(restart)
                    .sentTime(sentTime)
                    .taskJSON(taskJSON)
                    .responsURI(responsURI)
                    .deferredResult(result).build();

            if (result.getResult() == null) {
                Runnable workingThread = appCtx.getBean(SubmitTaskThread.class, parameters);
                taskExecutor.execute(workingThread);
            }

            while(result.getResult() == null) { }
        }

        return result;
    }

    @PostMapping(path = "{taskId}/cleaner", consumes = {MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    @Async
    public DeferredResult<Response> cleanIndexingDataSet(
            @PathVariable final String topologyName,
            @PathVariable final String taskId,
            /*check if input JSON is valid*/ @RequestBody final DataSetCleanerParameters cleanerParameters
    ) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        DeferredResult result = new DeferredResult();

        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    result.setResult(ResponseEntity.ok("The request was received successfully"));

                    if (cleanerParameters != null) {
                        LOGGER.info("cleaning dataset {} based on date: {}",
                                cleanerParameters.getDataSetId(), cleanerParameters.getCleaningDate());
                        DatasetCleaner datasetCleaner = new DatasetCleaner(cleanerParameters);
                        datasetCleaner.execute();
                        LOGGER.info("Dataset {} cleaned successfully", cleanerParameters.getDataSetId());
                        taskInfoDAO.setTaskStatus(Long.parseLong(taskId), "Completely process",
                                TaskState.PROCESSED.toString());
                    } else {
                        taskInfoDAO.dropTask(Long.parseLong(taskId), "cleaner parameters can not be null",
                                TaskState.DROPPED.toString());
                    }
                } catch (ParseException | DatasetCleaningException e) {
                    LOGGER.error("Dataset was not removed correctly. ", e);
                    taskInfoDAO.dropTask(Long.parseLong(taskId), e.getMessage(), TaskState.DROPPED.toString());
                }
            }
        }).start();

        while(result.getResult() == null) {
        }

        return result;
    }

    private void validateOutputDataSetsIfExist(DpsTask task) throws DpsTaskValidationException {
        List<String> dataSets = readDataSetsList(task.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS));
        if (dataSets != null) {
            for (String dataSetURL : dataSets) {
                try {
                    DataSet dataSet = parseDataSetURl(dataSetURL);
                    dataSetServiceClient.getDataSetRepresentationsChunk(dataSet.getProviderId(), dataSet.getId(), null);
                    validateProviderId(task, dataSet.getProviderId());
                } catch (MalformedURLException e) {
                    throw new DpsTaskValidationException("Validation failed. This output dataSet " + dataSetURL
                            + " can not be submitted because: " + e.getMessage());
                } catch (DataSetNotExistsException e) {
                    throw new DpsTaskValidationException("Validation failed. This output dataSet " + dataSetURL
                            + " Does not exist");
                } catch (Exception e) {
                    throw new DpsTaskValidationException("Unexpected exception happened while validating the dataSet: "
                            + dataSetURL + " because of: " + e.getMessage());
                }
            }
        }
    }

    private void validateProviderId(DpsTask task, String providerId) throws DpsTaskValidationException {
        String providedProviderId = task.getParameter(PluginParameterKeys.PROVIDER_ID);
        if (providedProviderId != null)
            if (!providedProviderId.equals(providerId))
                throw new DpsTaskValidationException("Validation failed. The provider id: " + providedProviderId
                        + " should be the same provider of the output dataSet: " + providerId);
    }


    private List<String> readDataSetsList(String listParameter) {
        if (listParameter == null)
            return null;
        return Arrays.asList(listParameter.split(","));
    }

    private DataSet parseDataSetURl(String url) throws MalformedURLException {
        UrlParser parser = new UrlParser(url);
        if (parser.isUrlToDataset()) {
            DataSet dataSet = new DataSet();
            dataSet.setId(parser.getPart(UrlPart.DATA_SETS));
            dataSet.setProviderId(parser.getPart(UrlPart.DATA_PROVIDERS));
            return dataSet;
        }
        throw new MalformedURLException("The dataSet URL is not formulated correctly");

    }


    /**
     * Retrieves a detailed report for the specified task.It will return info about
     * the first 100 resources unless you specified the needed chunk by using from&to parameters
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param from         The starting resource number should be bigger than 0
     * @param to           The ending resource number should be bigger than 0
     * @return Notification messages for the specified task.
     * @summary Retrieve task detailed report
     */
    @GetMapping(path = "{taskId}/reports/details", produces = {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    @Validated
    public List<SubTaskInfo> getTaskDetailedReport(
            @PathVariable String taskId,
            @PathVariable final String topologyName,
             @RequestParam(defaultValue = "1")  @Min(1) int from,
             @RequestParam(defaultValue = "100") @Min(1) int to)
                            throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);

        List<SubTaskInfo> result = null;
        if(!topologyName.equals(TopologiesNames.OAI_TOPOLOGY)) {
            result = reportService.getDetailedTaskReportBetweenChunks(taskId, from, to);
        } else {
            result = reportService.getDetailedTaskReportByPage(taskId, from, to);
        }
        return result;
    }


    /**
     * If error param is not specified it retrieves a report of all errors that occurred for the specified task. For each error
     * the number of occurrences is returned otherwise retrieves a report for a specific error that occurred in the specified task.
     * A sample of identifiers is returned as well. The number of identifiers is between 0 and ${maxIdentifiersCount}.
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param error        Error type.
     * @param idsCount     number of identifiers to retrieve
     * @return Errors that occurred for the specified task.
     * @summary Retrieve task detailed error report
     */
    @GetMapping(path = "{taskId}/reports/errors", produces = {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public TaskErrorsInfo getTaskErrorReport(
            @PathVariable String taskId,
            @PathVariable final String topologyName,
            @RequestParam String error,
            @RequestParam(defaultValue = "0") int idsCount)
                    throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);

        if (idsCount < 0 || idsCount > maxIdentifiersCount) {
            throw new IllegalArgumentException("Identifiers count parameter should be between 0 and " + maxIdentifiersCount);
        }
        if (error == null) {
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @return if the error report exists
     * @summary Check if the task has error report
     */
    @RequestMapping(method = { RequestMethod.HEAD }, path = "{taskId}/reports/errors")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public Boolean checkIfErrorReportExists(
            @PathVariable String taskId,
            @PathVariable final String topologyName)
                    throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        return reportService.checkIfReportExists(taskId);
    }

    /**
     * Retrieves a statistics report for the specified task. Only applicable for tasks executing {link eu.europeana.cloud.service.dps.storm.topologies.validation.topology.ValidationTopology}
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @return Statistics report for the specified task.
     * @summary Retrieve task statistics report
     */
    @GetMapping(path = "{taskId}/statistics", produces = {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public StatisticsReport getTaskStatisticsReport(
            @PathVariable String topologyName,
            @PathVariable  String taskId)
                    throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        return validationStatisticsService.getTaskStatisticsReport(Long.parseLong(taskId));
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param elementPath  <strong>REQUIRED</strong> Path for specific element.
     * @return List of distinct values and their occurrences.
     */

    @GetMapping(path = "{taskId}/reports/element", produces = {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public List<NodeReport> getElementsValues(
            @PathVariable String topologyName,
            @PathVariable  String taskId,
            @NotNull @RequestParam("path") String elementPath)
                    throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        return validationStatisticsService.getElementReport(Long.parseLong(taskId), elementPath);
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param username     <strong>REQUIRED</strong> Permissions are granted to the account with this unique username
     * @return Status code indicating whether the operation was successful or not.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Grant task permissions to user
     */

    @PostMapping(path = "{taskId}/permit")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @ReturnType("java.lang.Void")
    public Response grantPermissions(
            @PathVariable String topologyName,
            @PathVariable String taskId,
            @RequestParam String username) throws AccessDeniedOrTopologyDoesNotExistException {

        assertContainTopology(topologyName);

        if (taskId != null) {
            permissionManager.grantPermissionsForTask(taskId, username);
            return Response.ok().build();
        }
        return Response.notModified().build();
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param info         <strong>OPTIONAL</strong> The cause of the cancellation. If it was not specified a default cause 'Dropped by the user' will be provided
     * @return Status code indicating whether the operation was successful or not.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException   if taskId does not belong to the specified topology
     * @summary Kill task
     */

    @PostMapping(path = "{taskId}/kill")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', write)")
    public Response killTask(
            @PathVariable String topologyName,
            @PathVariable String taskId,
            @RequestParam(defaultValue = "Dropped by the user") String info)
                    throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        killService.killTask(Long.parseLong(taskId), info);
        return Response.ok("The task was killed because of " + info).build();
    }

   private URI buildTaskURI(StringBuffer base, DpsTask task) throws URISyntaxException {
        if(base.charAt(base.length()-1) != '/') {
            base.append('/');
        }
        base.append(task.getTaskId());
        return new URI(base.toString());
   }

    private void assertContainTopology(String topology) throws AccessDeniedOrTopologyDoesNotExistException {
        if (!topologyManager.containsTopology(topology)) {
            throw new AccessDeniedOrTopologyDoesNotExistException("The topology doesn't exist");
        }
    }

    private void validateTask(DpsTask task, String topologyName) throws DpsTaskValidationException {
        String taskType = specifyTaskType(task, topologyName);
        DpsTaskValidator validator = DpsTaskValidatorFactory.createValidator(taskType);
        validator.validate(task);
    }

    private String specifyTaskType(DpsTask task, String topologyName) throws DpsTaskValidationException {
        if (task.getDataEntry(FILE_URLS) != null) {
            return topologyName + "_" + FILE_URLS.name().toLowerCase();
        }
        if (task.getDataEntry(DATASET_URLS) != null) {
            return topologyName + "_" + DATASET_URLS.name().toLowerCase();
        }
        if (task.getDataEntry(REPOSITORY_URLS) != null) {
            return topologyName + "_" + REPOSITORY_URLS.name().toLowerCase();
        }
        throw new DpsTaskValidationException("Validation failed. Missing required data_entry");
    }
}