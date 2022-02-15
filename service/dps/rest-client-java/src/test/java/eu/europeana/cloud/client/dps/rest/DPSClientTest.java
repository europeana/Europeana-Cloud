package eu.europeana.cloud.client.dps.rest;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsException;
import eu.europeana.cloud.test.WiremockHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class DPSClientTest {

    private static final String BASE_URL_LOCALHOST = "http://localhost:8080/services/";
    private static final String BASE_URL = BASE_URL_LOCALHOST;
    private static final String USERNAME_ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "ecloud_admin";
    private static final String REGULAR_USER_NAME = "user";
    private static final String REGULAR_USER_PASSWORD = "ecloud_user";
    private static final String TOPOLOGY_NAME = "TopologyName";
    private static final String NOT_DEFINED_TOPOLOGY_NAME = "NotDefinedTopologyName";
    private static final String ERROR_TYPE = "92f19f57-d173-4e07-8fc9-2a6f0df42549";
    private static final String ERROR_MESSAGE = "Message";
    private static final int ERROR_OCCURRENCES = 5;
    private static final String RESOURCE_ID = "Resource id ";
    private static final String ADDITIONAL_INFORMATIONS = "Additional informations ";
    private static final int TASK_ID = 12345;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(8080));

    private DpsClient dpsClient;

    @Before
    public void init() {
    }

    @Test
    public final void shouldPermitAndSubmitTask()
            throws Exception {
        //given
        dpsClient = new DpsClient(BASE_URL, USERNAME_ADMIN, ADMIN_PASSWORD);
        DpsTask task = prepareDpsTask();

        new WiremockHelper(wireMockRule).stubPost(
                "/services/TopologyName/permit",
                200,
                null);
        new WiremockHelper(wireMockRule).stubPost(
                "/services/TopologyName/tasks",
                201,
                "http://localhost:8080/services/TopologyName/tasks/-2561925310040723252",
                null);

        //when
        dpsClient.topologyPermit(TOPOLOGY_NAME, REGULAR_USER_NAME);
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        long taskId = dpsClient.submitTask(task, TOPOLOGY_NAME);

        //then
        assertEquals(-2561925310040723252L, taskId);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public final void shouldThrowAnExceptionWhenCannotSubmitATask()
            throws Exception {
        //given
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        DpsTask task = prepareDpsTask();

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/services/TopologyName/tasks",
                405,
                "http://localhost:8080/services/TopologyName/tasks/-2561925310040723252",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //
        //when
        dpsClient.submitTask(task, TOPOLOGY_NAME);

        //then
        //throw an exception
    }


    @Test(expected = RuntimeException.class)
    public final void shouldThrowAnExceptionWhenReturnedTaskIdIsNotParsable()
            throws Exception {
        //given
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        DpsTask task = prepareDpsTask();

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/services/TopologyName/tasks",
                201,
                "http://localhost:8080/services/TopologyName/tasks/wrongTaskId",
                null);
        //

        //when
        dpsClient.submitTask(task, TOPOLOGY_NAME);

        //then
        //throw an exception
    }

    @Test
    public final void shouldNotBeAbleToPermitUserForNotDefinedTopology()
            throws Exception {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        //given
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/services/NotDefinedTopologyName/permit",
                405,
                "http://localhost:8080/services/TopologyName/tasks/wrongTaskId",
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>The topology doesn't exist</details><errorCode>ACCESS_DENIED_OR_TOPOLOGY_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //

        try {
            //when
            dpsClient.topologyPermit(NOT_DEFINED_TOPOLOGY_NAME, "user");
            fail();
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
            assertThat(e.getLocalizedMessage(), equalTo("The topology doesn't exist"));
        }
    }

    @Test
    public final void shouldReturnedProgressReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        TaskInfo taskInfo = TaskInfo.builder()
                .id(TASK_ID)
                .topologyName(TOPOLOGY_NAME)
                .state(TaskState.PROCESSED)
                .expectedRecordsNumber(0)
                .processedRecordsCount(0)
                .processedErrorsCount(0)
                .build();

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/services/TopologyName/tasks/12345/progress",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><taskInfo><expectedSize>1</expectedSize><id>12345</id><info></info><processedElementCount>0</processedElementCount><state>PROCESSED</state><topologyName>TopologyName</topologyName></taskInfo>");
        //
        assertThat(dpsClient.getTaskProgress(TOPOLOGY_NAME, TASK_ID), is(taskInfo));

    }

    @Test
    public final void shouldKillTask() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/services/TopologyName/tasks/12345/kill",
                200,
                "The task was killed because of Dropped by the user");
        //
        String responseMessage = dpsClient.killTask(TOPOLOGY_NAME, TASK_ID, null);
        assertEquals("The task was killed because of Dropped by the user", responseMessage);

    }

    @Test
    public final void shouldKillTaskWithSpecificInfo() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/services/TopologyName/tasks/12345/kill?info=Aggregator-choice",
                200,
                "The task was killed because of Aggregator-choice");
        //
        String responseMessage = dpsClient.killTask(TOPOLOGY_NAME, TASK_ID, "Aggregator-choice");
        assertEquals("The task was killed because of Aggregator-choice", responseMessage);

    }


    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public final void shouldThrowExceptionWhenKillingNonExistingTaskTest() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        long nonExistedTaskId = 1111L;
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/services/TopologyName/tasks/1111/kill",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //
        dpsClient.killTask(TOPOLOGY_NAME, nonExistedTaskId, null);


    }

    @Test(expected = AccessDeniedOrTopologyDoesNotExistException.class)
    public final void shouldThrowExceptionWhenKillingTaskForNonExistedTopologyTest() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/services/NotDefinedTopologyName/tasks/12345/kill",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>The topology doesn't exist</details><errorCode>ACCESS_DENIED_OR_TOPOLOGY_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //
        dpsClient.killTask(NOT_DEFINED_TOPOLOGY_NAME, TASK_ID, null);
    }

    @Test
    public final void shouldReturnedDetailsReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        SubTaskInfo subTaskInfo = new SubTaskInfo(1, "resource", RecordState.SUCCESS, "", "", null, "result");
        List<SubTaskInfo> taskInfoList = new ArrayList<>(1);
        taskInfoList.add(subTaskInfo);
        //
        new WiremockHelper(wireMockRule).stubGet(
                "/services/TopologyName/tasks/12345/reports/details",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><subTaskInfos><subTaskInfo><additionalInformations></additionalInformations> <info></info> <resource>resource</resource> <resourceNum>1</resourceNum> <resultResource>result</resultResource> <recordState>SUCCESS</recordState></subTaskInfo></subTaskInfos>");
        //

        assertThat(dpsClient.getDetailedTaskReport(TOPOLOGY_NAME, TASK_ID), is(taskInfoList));

    }

    @Test
    public final void shouldReturnedGeneralErrorReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        TaskErrorsInfo report = createErrorInfo(false);
        //
        new WiremockHelper(wireMockRule).stubGet(
                "/services/TopologyName/tasks/12345/reports/errors?idsCount=0",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><taskErrorsInfo><errors><errorType>92f19f57-d173-4e07-8fc9-2a6f0df42549</errorType><message>Message</message><occurrences>5</occurrences></errors><id>12345</id></taskErrorsInfo>");
        //
        assertThat(dpsClient.getTaskErrorsReport(TOPOLOGY_NAME, TASK_ID, null, 0), is(report));

    }

    @Test
    public final void shouldReturnTrueWhenErrorsReportExists() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        //
        new WiremockHelper(wireMockRule).stubHead("/services/TopologyName/tasks/12345/reports/errors", 200);
        //
        assertTrue(dpsClient.checkIfErrorReportExists(TOPOLOGY_NAME, TASK_ID));
    }

    @Test
    public final void shouldReturnFalseWhenErrorsReportDoesNotExists() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        //
        new WiremockHelper(wireMockRule).stubHead("/services/TopologyName/tasks/12345/reports/errors", 405);
        //
        assertFalse(dpsClient.checkIfErrorReportExists(TOPOLOGY_NAME, TASK_ID));
    }

    @Test
    public final void shouldReturnedSpecificErrorReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        TaskErrorsInfo report = createErrorInfo(true);
        new WiremockHelper(wireMockRule).stubGet("/services/TopologyName/tasks/12345/reports/errors?error=92f19f57-d173-4e07-8fc9-2a6f0df42549&idsCount=100",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><taskErrorsInfo>\n" +
                        "          <errors>    <errorDetails>\n" +
                        "                  <additionalInfo>Additional informations 0</additionalInfo>\n" +
                        "                  <identifier>Resource id 0</identifier>\n" +
                        "              </errorDetails>\n" +
                        "              <errorDetails>\n" +
                        "                  <additionalInfo>Additional informations 1</additionalInfo>\n" +
                        "                  <identifier>Resource id 1</identifier>\n" +
                        "              </errorDetails>\n" +
                        "              <errorDetails>\n" +
                        "                  <additionalInfo>Additional informations 2</additionalInfo>\n" +
                        "                  <identifier>Resource id 2</identifier>\n" +
                        "              </errorDetails>\n" +
                        "              <errorDetails>\n" +
                        "                  <additionalInfo>Additional informations 3</additionalInfo>\n" +
                        "                  <identifier>Resource id 3</identifier>\n" +
                        "              </errorDetails>\n" +
                        "              <errorDetails>\n" +
                        "                  <additionalInfo>Additional informations 4</additionalInfo>\n" +
                        "                  <identifier>Resource id 4</identifier>\n" +
                        "              </errorDetails>\n" +
                        "              <errorType>92f19f57-d173-4e07-8fc9-2a6f0df42549</errorType>\n" +
                        "              <message>Message</message>\n" +
                        "              <occurrences>5</occurrences>\n" +
                        "          </errors>\n" +
                        "          <id>12345</id>\n" +
                        "          </taskErrorsInfo>");
        assertThat(dpsClient.getTaskErrorsReport(TOPOLOGY_NAME, TASK_ID, ERROR_TYPE, 100), is(report));

    }

    @Test
    public void shouldReturnStatistics() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        //
        new WiremockHelper(wireMockRule).stubGetWithJsonContent(
                "/services/TopologyName/tasks/12345/statistics",
                200,
                "{\"nodeStatistics\":[{\"attributesStatistics\":[],\"occurrence\":2,\"parentXpath\":\"\",\"value\":\"\",\"xpath\":\"//rdf:RDF\"}]," +
                        "\"taskId\":12345}");
        //
        StatisticsReport report = dpsClient.getTaskStatisticsReport(TOPOLOGY_NAME, TASK_ID);
        assertNotNull(report.getNodeStatistics());
        assertEquals(TASK_ID, report.getTaskId());
    }


    @Test
    public void shouldGetTheElementReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        //
        new WiremockHelper(wireMockRule).stubGetWithJsonContent(
                "/services/TopologyName/tasks/12345/reports/element?path=%2F%2Frdf%3ARDF%2Fedm%3APlace%2Fskos%3AprefLabel",
                200,
                "[{\"attributeStatistics\":[{\"name\":\"//rdf:RDF/edm:Place/skos:prefLabel/@xml:lang\",\"occurrence\":10,\"value\":\"en\"}],\"nodeValue\":\"Lattakia\",\"occurrence\":10}]");
        //

        List<NodeReport> nodeReports = dpsClient.getElementReport(TOPOLOGY_NAME, TASK_ID, "//rdf:RDF/edm:Place/skos:prefLabel");
        assertNotNull(nodeReports);
        assertEquals(1, nodeReports.size());
        assertEquals("Lattakia", nodeReports.get(0).getNodeValue());
        assertEquals(10, nodeReports.get(0).getOccurrence());
        List<AttributeStatistics> attributes = nodeReports.get(0).getAttributeStatistics();
        assertNotNull(attributes);
        assertEquals(1, attributes.size());
        assertEquals("//rdf:RDF/edm:Place/skos:prefLabel/@xml:lang", attributes.get(0).getName());
        assertEquals(10, attributes.get(0).getOccurrence());
        assertEquals("en", attributes.get(0).getValue());
    }


    @Test(expected = AccessDeniedOrTopologyDoesNotExistException.class)
    public void shouldThrowExceptionForStatistics() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        //
        new WiremockHelper(wireMockRule).stubGet(
                "/services/NotDefinedTopologyName/tasks/12345/statistics",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>The topology doesn't exist</details><errorCode>ACCESS_DENIED_OR_TOPOLOGY_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //
        dpsClient.getTaskStatisticsReport(NOT_DEFINED_TOPOLOGY_NAME, TASK_ID);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowExceptionForStatisticsWhenTaskIdIsUnAccessible() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        //
        new WiremockHelper(wireMockRule).stubGet(
                "/services/TopologyName/tasks/12345/statistics",
                403,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //
        dpsClient.getTaskStatisticsReport(TOPOLOGY_NAME, TASK_ID);
    }

    private DpsTask prepareDpsTask() {
        DpsTask task = new DpsTask("oaiPmhHarvestingTask");
        task.addDataEntry(REPOSITORY_URLS, List.of("http://example.com/oai-pmh-repository.xml"));
        task.setHarvestingDetails(new OAIPMHHarvestingDetails("Schema"));
        return task;
    }

    private TaskErrorsInfo createErrorInfo(boolean specific) {
        TaskErrorsInfo info = new TaskErrorsInfo();
        info.setId(DPSClientTest.TASK_ID);
        List<TaskErrorInfo> errors = new ArrayList<>(1);
        TaskErrorInfo error = new TaskErrorInfo();
        error.setOccurrences(ERROR_OCCURRENCES);
        error.setErrorType(ERROR_TYPE);
        error.setMessage(ERROR_MESSAGE);
        errors.add(error);
        info.setErrors(errors);

        if (specific) {
            List<ErrorDetails> errorDetails = new ArrayList<>(1);
            error.setErrorDetails(errorDetails);

            for (int i = 0; i < ERROR_OCCURRENCES; i++) {
                errorDetails.add(new ErrorDetails(RESOURCE_ID + i, ADDITIONAL_INFORMATIONS + i));
            }
        }
        return info;
    }
}
