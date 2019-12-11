package eu.europeana.cloud.service.dps.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.rest.oaiharvest.IdentifierHarvester;
import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIItem;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaningException;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.utils.DpsTaskValidatorFactory;
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
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.service.dps.InputDataType.*;

/**
 * Resource to fetch / submit Tasks to the DPS service
 */
@Path("/{topologyName}/tasks")
@Component
@Scope("request")
public class TopologyTasksResource {
    @Value("${maxIdentifiersCount}")
    private int maxIdentifiersCount;

    @Autowired
    ApplicationContext context;

    @Autowired
    private TaskExecutionReportService reportService;

    @Autowired
    private ValidationStatisticsReportService validationStatisticsService;

    @Autowired
    private TaskExecutionSubmitService submitService;

    @Autowired
    private RecordExecutionSubmitService recordSubmitService;

    @Autowired
    private TaskExecutionKillService killService;

    @Autowired
    private TopologyManager topologyManager;

    @Autowired
    private PermissionManager permissionManager;

    @Autowired
    private String mcsLocation;

    @Autowired
    private FileServiceClient fileServiceClient;

    @Autowired
    private DataSetServiceClient dataSetServiceClient;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private TasksByStateDAO taskDAO;

    @Autowired
    private FilesCounterFactory filesCounterFactory;

    @Autowired
    private String applicationIdentifier;

    private final static String TOPOLOGY_PREFIX = "Topology";

    public final static String TASK_PREFIX = "DPS_Task";

    public final static String HTTP_TOPOLOGY = "http_topology";

    private static final int UNKNOWN_EXPECTED_SIZE = -1;

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
    @GET
    @Path("{taskId}/progress")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public TaskInfo getTaskProgress(
            @PathParam("topologyName") String topologyName,
            @PathParam("taskId") String taskId) throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        TaskInfo progress = reportService.getTaskProgress(taskId);
        return progress;
    }


    //protected static volatile TaskStatusChecker taskStatusChecker;


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
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    public Response submitTask(@Suspended final AsyncResponse asyncResponse,
                               final DpsTask task,
                               @PathParam("topologyName") final String topologyName,
                               @Context final UriInfo uriInfo,
                               @HeaderParam("Authorization") final String authorizationHeader
    ) throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
        if (task != null) {
            LOGGER.info("Submitting task");
            assertContainTopology(topologyName);
            validateTask(task, topologyName);
            validateOutputDataSetsIfExist(task);
            final Date sentTime = new Date();
            final String taskJSON = new ObjectMapper().writeValueAsString(task);

            TaskStatusChecker.init(taskInfoDAO);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        String createdTaskUrl = buildTaskUrl(uriInfo, task, topologyName);
                        Response response = Response.created(new URI(createdTaskUrl)).build();
                        insertTask(task.getTaskId(), topologyName, 0, TaskState.PENDING.toString(), "The task is in a pending mode, it is being processed before submission", sentTime, taskJSON);
                        permissionManager.grantPermissionsForTask(String.valueOf(task.getTaskId()));
                        asyncResponse.resume(response);
                        LOGGER.info("The task is in a pending mode");
                        int expectedSize = getFilesCountInsideTask(task, topologyName);
                        if (expectedSize == 0)
                            insertTask(task.getTaskId(), topologyName, 0, TaskState.DROPPED.toString(), "The task doesn't include any records", sentTime, taskJSON);
                        else {
                            task.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, authorizationHeader);
                            submitService.submitTask(task, topologyName);
                            LOGGER.info("Task submitted successfully");

                            insertTask(task.getTaskId(), topologyName, expectedSize, TaskState.SENT.toString(), "", sentTime, taskJSON);

                            if (!TaskStatusChecker.getTaskStatusChecker().hasKillFlag(task.getTaskId())) {
                                OAIPMHHarvestingDetails oaipmhHarvestingDetails = task.getHarvestingDetails();
                                if (oaipmhHarvestingDetails == null)
                                    oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
                                OAIItem oaiItem = new OAIItem(
                                        task.getTaskId(),
                                        task.getTaskName(),
                                        task.getDataEntry(InputDataType.REPOSITORY_URLS).get(0), null, task.getParameters(), task.getOutputRevision(), oaipmhHarvestingDetails);

                                IdentifierHarvester ih = new IdentifierHarvester(topologyName, oaiItem, taskInfoDAO, recordSubmitService, TaskStatusChecker.getTaskStatusChecker());
                                ih.harvest();
                            } else {
                                LOGGER.info("Skipping DROPPED task {}", task.getTaskId());
                            }
                        }

                    } catch (URISyntaxException e) {
                        LOGGER.error("Task submission failed");
                        Response response = Response.serverError().build();
                        insertTask(task.getTaskId(), topologyName, 0, TaskState.DROPPED.toString(), e.getMessage(), sentTime, taskJSON);
                        asyncResponse.resume(response);
                    } catch (TaskSubmissionException e) {
                        LOGGER.error("Task submission failed: {}", e.getMessage());
                        insertTask(task.getTaskId(), topologyName, 0, TaskState.DROPPED.toString(), e.getMessage(), sentTime, taskJSON);
                    } catch (Exception e) {
                        String fullStacktrace = ExceptionUtils.getStackTrace(e);
                        LOGGER.error("Task submission failed: {}", fullStacktrace);
                        insertTask(task.getTaskId(), topologyName, 0, TaskState.DROPPED.toString(), fullStacktrace, sentTime, taskJSON);
                        Response response = Response.serverError().build();
                        asyncResponse.resume(response);
                    }
                }
            }).start();

        }
        return Response.notModified().build();
    }

    private void insertTask(long taskId, String topologyName, int expectedSize, String state, String info, Date sentTime, String taskInformations) {
        taskInfoDAO.insert(taskId, topologyName, expectedSize, state, info, sentTime, taskInformations);
        taskDAO.insert(state, topologyName, taskId, applicationIdentifier);
    }

    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    @Path("{taskId}/cleaner")
    public void cleanIndexingDataSet(@Suspended final AsyncResponse asyncResponse,
                                     @PathParam("topologyName") final String topologyName,
                                     @PathParam("taskId") final String taskId,
                                     final DataSetCleanerParameters cleanerParameters
    ) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    asyncResponse.resume("The request was received successfully");
                    if (cleanerParameters != null) {
                        LOGGER.info("cleaning dataset {} based on date: {}", cleanerParameters.getDataSetId(), cleanerParameters.getCleaningDate());
                        DatasetCleaner datasetCleaner = new DatasetCleaner(cleanerParameters);
                        datasetCleaner.execute();
                        LOGGER.info("Dataset {} cleaned successfully", cleanerParameters.getDataSetId());
                        taskInfoDAO.setTaskStatus(Long.parseLong(taskId), "Completely process", TaskState.PROCESSED.toString());
                    } else {
                        taskInfoDAO.dropTask(Long.parseLong(taskId), "cleaner parameters can not be null", TaskState.DROPPED.toString());
                    }
                } catch (ParseException e) {
                    LOGGER.error("Dataset was not removed correctly. ", e);
                    taskInfoDAO.dropTask(Long.parseLong(taskId), e.getMessage(), TaskState.DROPPED.toString());
                } catch (DatasetCleaningException e) {
                    LOGGER.error("Dataset was not removed correctly. ", e);
                    taskInfoDAO.dropTask(Long.parseLong(taskId), e.getMessage(), TaskState.DROPPED.toString());
                }
            }
        }).start();
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
                    throw new DpsTaskValidationException("Validation failed. This output dataSet " + dataSetURL + " can not be submitted because: " + e.getMessage());
                } catch (DataSetNotExistsException e) {
                    throw new DpsTaskValidationException("Validation failed. This output dataSet " + dataSetURL + " Does not exist");
                } catch (Exception e) {
                    throw new DpsTaskValidationException("Unexpected exception happened while validating the dataSet: " + dataSetURL + " because of: " + e.getMessage());
                }
            }
        }
    }

    private void validateProviderId(DpsTask task, String providerId) throws DpsTaskValidationException {
        String providedProviderId = task.getParameter(PluginParameterKeys.PROVIDER_ID);
        if (providedProviderId != null)
            if (!providedProviderId.equals(providerId))
                throw new DpsTaskValidationException("Validation failed. The provider id: " + providedProviderId + " should be the same provider of the output dataSet: " + providerId);

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
    @GET
    @Path("{taskId}/reports/details")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public List<SubTaskInfo> getTaskDetailedReport(@PathParam("taskId") String taskId, @PathParam("topologyName") final String topologyName, @Min(1) @DefaultValue("1") @QueryParam("from") int from, @Min(1) @DefaultValue("100") @QueryParam("to") int to) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        List<SubTaskInfo> taskInfo = reportService.getDetailedTaskReportBetweenChunks(taskId, from, to);
        return taskInfo;
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
    @GET
    @Path("{taskId}/reports/errors")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public TaskErrorsInfo getTaskErrorReport(@PathParam("taskId") String taskId, @PathParam("topologyName") final String topologyName, @QueryParam("error") String error, @DefaultValue("0") @QueryParam("idsCount") int idsCount) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
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
    @HEAD
    @Path("{taskId}/reports/errors")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public Boolean checkIfErrorReportExists(@PathParam("taskId") String taskId, @PathParam("topologyName") final String topologyName) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
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
    @GET
    @Path("{taskId}/statistics")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public StatisticsReport getTaskStatisticsReport(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
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

    @GET
    @Path("{taskId}/reports/element")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public List<NodeReport> getElementsValues(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId, @NotNull @QueryParam("path") String elementPath) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
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

    @POST
    @Path("{taskId}/permit")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @ReturnType("java.lang.Void")
    public Response grantPermissions(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId,
                                     @FormParam("username") String username) throws AccessDeniedOrTopologyDoesNotExistException {

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

    @POST
    @Path("{taskId}/kill")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', write)")
    public Response killTask(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId, @QueryParam("info") @DefaultValue("Dropped by the user") String info) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        killService.killTask(Long.parseLong(taskId), info);
        return Response.ok("The task was killed because of " + info).build();

    }

    private String buildTaskUrl(UriInfo uriInfo, DpsTask task, String topologyName) {

        StringBuilder taskUrl = new StringBuilder()
                .append(uriInfo.getBaseUri().toString())
                .append(topologyName)
                .append("/tasks/")
                .append(task.getTaskId());

        return taskUrl.toString();
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

    /**
     * @return The number of files inside the task.
     */
    private int getFilesCountInsideTask(DpsTask submittedTask, String topologyName) throws TaskSubmissionException {
        if (topologyName.equals(HTTP_TOPOLOGY))
            return UNKNOWN_EXPECTED_SIZE;
        String taskType = getTaskType(submittedTask);
        FilesCounter filesCounter = filesCounterFactory.createFilesCounter(taskType);
        return filesCounter.getFilesCount(submittedTask);
    }


    //get TaskType
    private String getTaskType(DpsTask task) {
        //TODO sholud be done in more error prone way
        final InputDataType first = task.getInputData().keySet().iterator().next();
        return first.name();
    }
}