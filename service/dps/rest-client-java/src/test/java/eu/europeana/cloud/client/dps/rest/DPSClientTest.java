package eu.europeana.cloud.client.dps.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsException;
import eu.europeana.cloud.test.WiremockHelper;
import org.glassfish.jersey.uri.UriComponent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.HttpHeaders.LOCATION;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class DPSClientTest {

  private static final String BASE_URL = "http://localhost:8181/services/";
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
  private static final String ADDITIONAL_INFORMATION = "Additional information ";
  private static final int TASK_ID = 12345;

  @RegisterExtension
  static WireMockExtension wireMockExtension =
          WireMockExtension.newInstance()
                  .options(wireMockConfig().port(8181))
                  .build();

  private DpsClient dpsClient;


  @Test
  final void shouldPermitAndSubmitTask()
          throws Exception {
    //given
    dpsClient = new DpsClient(BASE_URL, USERNAME_ADMIN, ADMIN_PASSWORD);
    DpsTask task = prepareDpsTask();

    new WiremockHelper(wireMockExtension).stubPost(
            "/services/TopologyName/permit",
            200,
            null);
    wireMockExtension.stubFor(post(urlEqualTo("/services/TopologyName/tasks"))
        .willReturn(aResponse()
            .withStatus(201)
            .withHeader(CONTENT_TYPE, APPLICATION_JSON)
            .withHeader(LOCATION, "http://localhost:8080/services/TopologyName/tasks/-2561925310040723252")
            .withBody("{\"taskId\":-2561925310040723252}")));
    new WiremockHelper(wireMockExtension).stubPut(
        "/services/TopologyName/tasks/-2561925310040723252/started",
        204);
    //when
    dpsClient.topologyPermit(TOPOLOGY_NAME, REGULAR_USER_NAME);
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    DpsTask resultTask = dpsClient.createTask(task, TOPOLOGY_NAME);

    //then
    assertEquals(-2561925310040723252L, resultTask.getTaskId());

    dpsClient.startTask(TOPOLOGY_NAME,resultTask.getTaskId());
  }

  @Test
  final void shouldThrowAnExceptionWhenCannotSubmitATask() {
    //given
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    DpsTask task = prepareDpsTask();

    //
    new WiremockHelper(wireMockExtension).stubPost(
            "/services/TopologyName/tasks",
            405,
            "http://localhost:8080/services/TopologyName/tasks/-2561925310040723252",
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //
    Assertions.assertThrows(AccessDeniedOrObjectDoesNotExistException.class, () ->
            dpsClient.createTask(task, TOPOLOGY_NAME));
  }

  @Test
  final void shouldThrowAnExceptionWhenReturnedTaskIsNotParsable() {
    //given
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    DpsTask task = prepareDpsTask();

    //
    wireMockExtension.stubFor(post(urlEqualTo("/services/TopologyName/tasks"))
        .willReturn(aResponse()
            .withStatus(201)
            .withHeader(CONTENT_TYPE, APPLICATION_JSON)
            .withHeader(LOCATION, "http://localhost:8080/services/TopologyName/tasks/-2561925310040723252")
            .withBody("Broken task entity")));
    //

    //when
    Assertions.assertThrows(DpsException.class, () ->
        dpsClient.createTask(task, TOPOLOGY_NAME));

    //then
    //throw an exception
  }

  @Test
  final void shouldNotBeAbleToPermitUserForNotDefinedTopology()
          throws Exception {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    //given
    //
    new WiremockHelper(wireMockExtension).stubPost(
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
  final void shouldReturnedProgressReport() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);

    Date sent = new Date(1000);
    Date start = new Date(2000);
    Date finish = new Date(3000);

    TaskInfo expected = TaskInfo.builder()
            .id(TASK_ID)
            .topologyName(TOPOLOGY_NAME)
            .state(EngineTaskState.PROCESSED)
            .stateDescription("done")

            .sentTimestamp(sent)
            .startTimestamp(start)
            .finishTimestamp(finish)

            .expectedRecords(10)
            .successRecords(5)
            .warningRecords(1)
            .failRecords(2)
            .duplicateRecords(1)
            .unchangedRecords(1)
            .processedRecords(10)

            .postProcessedRecords(8)
            .expectedPostProcessedRecords(9)

            .expectedDepublishRecords(4)
            .successDepublishRecords(2)
            .failDepublishRecords(1)
            .processedDepublishRecords(3)

            .definition("definition-json")
            .build();

    new WiremockHelper(wireMockExtension).stubGet(
            "/services/TopologyName/tasks/12345/progress",
            200,
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                    + "<taskInfo>"
                    + "<id>12345</id>"
                    + "<topologyName>TopologyName</topologyName>"
                    + "<sentTimestamp>1970-01-01T00:00:01.000Z</sentTimestamp>"
                    + "<startTimestamp>1970-01-01T00:00:02.000Z</startTimestamp>"
                    + "<finishTimestamp>1970-01-01T00:00:03.000Z</finishTimestamp>"

                    + "<ecloudTaskProgress>"
                    + "<expectedRecords>10</expectedRecords>"
                    + "<successRecords>5</successRecords>"
                    + "<warningRecords>1</warningRecords>"
                    + "<failRecords>2</failRecords>"
                    + "<duplicateRecords>1</duplicateRecords>"
                    + "<unchangedRecords>1</unchangedRecords>"
                    + "<processedRecords>10</processedRecords>"

                    + "<postProcessedRecords>8</postProcessedRecords>"
                    + "<expectedPostProcessedRecords>9</expectedPostProcessedRecords>"

                    + "<expectedDepublishRecords>4</expectedDepublishRecords>"
                    + "<successDepublishRecords>2</successDepublishRecords>"
                    + "<failDepublishRecords>1</failDepublishRecords>"
                    + "<processedDepublishRecords>3</processedDepublishRecords>"

                    + "<engineTaskState>PROCESSED</engineTaskState>"
                    + "<engineTaskStateInfo>done</engineTaskStateInfo>"
                    + "</ecloudTaskProgress>"

                    + "<definition>definition-json</definition>"
                    + "</taskInfo>"
    );

    TaskInfo actual = dpsClient.getTaskProgress(TOPOLOGY_NAME, TASK_ID);

    assertEquals(expected, actual);
  }

  @Test
  final void shouldKillTask() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
    //
    new WiremockHelper(wireMockExtension).stubPost(
            "/services/TopologyName/tasks/12345/kill",
            200,
            "The task was killed because of Dropped by the user");
    //
    String responseMessage = dpsClient.killTask(TOPOLOGY_NAME, TASK_ID, null);
    assertEquals("The task was killed because of Dropped by the user", responseMessage);
  }

  @Test
  final void shouldKillTaskWithSpecificInfo() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
    //
    new WiremockHelper(wireMockExtension).stubPost(
            "/services/TopologyName/tasks/12345/kill?info=Aggregator-choice",
            200,
            "The task was killed because of Aggregator-choice");
    //
    String responseMessage = dpsClient.killTask(TOPOLOGY_NAME, TASK_ID, "Aggregator-choice");
    assertEquals("The task was killed because of Aggregator-choice", responseMessage);

  }


  @Test
  final void shouldThrowExceptionWhenKillingNonExistingTaskTest() {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
    long nonExistedTaskId = 1111L;
    //
    new WiremockHelper(wireMockExtension).stubPost(
            "/services/TopologyName/tasks/1111/kill",
            405,
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    Assertions.assertThrows(AccessDeniedOrObjectDoesNotExistException.class, () ->
            dpsClient.killTask(TOPOLOGY_NAME, nonExistedTaskId, null));


  }

  @Test
  final void shouldThrowExceptionWhenKillingTaskForNonExistedTopologyTest() {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
    //
    new WiremockHelper(wireMockExtension).stubPost(
            "/services/NotDefinedTopologyName/tasks/12345/kill",
            405,
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>The topology doesn't exist</details><errorCode>ACCESS_DENIED_OR_TOPOLOGY_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //

    Assertions.assertThrows(AccessDeniedOrTopologyDoesNotExistException.class, () ->
            dpsClient.killTask(NOT_DEFINED_TOPOLOGY_NAME, TASK_ID, null));
  }

  @Test
  final void shouldReturnedDetailsReport() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    SubTaskInfo subTaskInfo = new SubTaskInfo(1, "resource", RecordState.SUCCESS, "", "", null, 0L, "result");
    List<SubTaskInfo> taskInfoList = new ArrayList<>(1);
    taskInfoList.add(subTaskInfo);
    //
    new WiremockHelper(wireMockExtension).stubGet(
            "/services/TopologyName/tasks/12345/reports/details",
            200,
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><subTaskInfos><subTaskInfo><additionalInformations></additionalInformations> <info></info> <resource>resource</resource> <resourceNum>1</resourceNum> <resultResource>result</resultResource> <recordState>SUCCESS</recordState></subTaskInfo></subTaskInfos>");
    //

    assertThat(dpsClient.getDetailedTaskReport(TOPOLOGY_NAME, TASK_ID), is(taskInfoList));

  }

  @Test
  final void shouldReturnedGeneralErrorReport() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    TaskErrorsInfo report = createErrorInfo(false);
    //
    new WiremockHelper(wireMockExtension).stubGet(
            "/services/TopologyName/tasks/12345/reports/errors?idsCount=0",
            200,
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><taskErrorsInfo><errors><errorType>92f19f57-d173-4e07-8fc9-2a6f0df42549</errorType><message>Message</message><occurrences>5</occurrences></errors><id>12345</id></taskErrorsInfo>");
    //
    assertThat(dpsClient.getTaskErrorsReport(TOPOLOGY_NAME, TASK_ID, null, 0), is(report));

  }

  @Test
  final void shouldReturnTrueWhenErrorsReportExists() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    //
    new WiremockHelper(wireMockExtension).stubHead("/services/TopologyName/tasks/12345/reports/errors", 200);
    //
    assertTrue(dpsClient.checkIfErrorReportExists(TOPOLOGY_NAME, TASK_ID));
  }

  @Test
  final void shouldReturnFalseWhenErrorsReportDoesNotExists() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    //
    new WiremockHelper(wireMockExtension).stubHead("/services/TopologyName/tasks/12345/reports/errors", 405);
    //
    assertFalse(dpsClient.checkIfErrorReportExists(TOPOLOGY_NAME, TASK_ID));
  }

  @Test
  final void shouldReturnedSpecificErrorReport() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    TaskErrorsInfo report = createErrorInfo(true);
    new WiremockHelper(wireMockExtension).stubGet(
            "/services/TopologyName/tasks/12345/reports/errors?error=92f19f57-d173-4e07-8fc9-2a6f0df42549&idsCount=100",
            200,
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><taskErrorsInfo>\n" +
                    "          <errors>    <errorDetails>\n" +
                    "                  <additionalInfo>Additional information 0</additionalInfo>\n" +
                    "                  <identifier>Resource id 0</identifier>\n" +
                    "              </errorDetails>\n" +
                    "              <errorDetails>\n" +
                    "                  <additionalInfo>Additional information 1</additionalInfo>\n" +
                    "                  <identifier>Resource id 1</identifier>\n" +
            "              </errorDetails>\n" +
            "              <errorDetails>\n" +
            "                  <additionalInfo>Additional information 2</additionalInfo>\n" +
            "                  <identifier>Resource id 2</identifier>\n" +
            "              </errorDetails>\n" +
            "              <errorDetails>\n" +
            "                  <additionalInfo>Additional information 3</additionalInfo>\n" +
            "                  <identifier>Resource id 3</identifier>\n" +
            "              </errorDetails>\n" +
            "              <errorDetails>\n" +
            "                  <additionalInfo>Additional information 4</additionalInfo>\n" +
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
  void shouldReturnStatistics() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    //
    new WiremockHelper(wireMockExtension).stubGetWithJsonContent(
            "/services/TopologyName/tasks/12345/statistics",
            200,
            "{\"nodeStatistics\":[{\"attributesStatistics\":[],\"occurrence\":2,\"parentXpath\":\"\",\"value\":\"\",\"xpath\":\"//rdf:RDF\"}],"
                    +
                    "\"taskId\":12345}");
    //
    StatisticsReport report = dpsClient.getTaskStatisticsReport(TOPOLOGY_NAME, TASK_ID);
    assertNotNull(report.getNodeStatistics());
    assertEquals(TASK_ID, report.getTaskId());
  }


  @Test
  void shouldGetTheElementReport() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    //
    new WiremockHelper(wireMockExtension).stubGetWithJsonContent(
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


  @Test
  void shouldThrowExceptionForStatistics() {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    //
    new WiremockHelper(wireMockExtension).stubGet(
            "/services/NotDefinedTopologyName/tasks/12345/statistics",
            405,
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>The topology doesn't exist</details><errorCode>ACCESS_DENIED_OR_TOPOLOGY_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //

    Assertions.assertThrows(AccessDeniedOrTopologyDoesNotExistException.class,
            () -> dpsClient.getTaskStatisticsReport(NOT_DEFINED_TOPOLOGY_NAME, TASK_ID));
  }

  @Test
  void shouldThrowExceptionForStatisticsWhenTaskIdIsUnAccessible() {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
    //
    new WiremockHelper(wireMockExtension).stubGet(
            "/services/TopologyName/tasks/12345/statistics",
            403,
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");

    Assertions.assertThrows(AccessDeniedOrObjectDoesNotExistException.class,
            () -> dpsClient.getTaskStatisticsReport(TOPOLOGY_NAME, TASK_ID));
  }

  @Test
  void shouldReturnFoundPublishedDatasetRecords() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
    //
    prepareMocksForSearchPublishedDatasetRecords(false, "12345");
    //
    var foundRecords = dpsClient.searchPublishedDatasetRecords("12345", Arrays.asList("id1", "id2", "id3", "id4"));
    //
    assertEquals(2, foundRecords.size());
  }


  @Test
  void shouldThrowNPEForNullDatasetId() {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
    //
    var list = Arrays.asList("id1", "id2", "id3", "id4");
    assertThrows(NullPointerException.class,
            () -> dpsClient.searchPublishedDatasetRecords(null, list));
  }

  @Test
  void shouldThrowDPSExceptionForNullRecordIds() {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
    //
    prepareMocksForSearchPublishedDatasetRecords(true, "12345");
    //
    assertThrows(DpsException.class,
            () -> dpsClient.searchPublishedDatasetRecords("12345", null));
  }

  @Test
  void shouldReturnNPEForNullDatasetIdAndRecordIds() {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
    //
    prepareMocksForSearchPublishedDatasetRecords(true, "12345");
    //
    assertThrows(NullPointerException.class,
            () -> dpsClient.searchPublishedDatasetRecords(null, null));
  }

  @Test
  void shouldReturnForAtypicalDatasetIdChars() throws DpsException {
    dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
    //
    prepareMocksForSearchPublishedDatasetRecords(false, "!@#/$%^");
    //
    var foundRecords = dpsClient.searchPublishedDatasetRecords("!@#/$%^", Arrays.asList("id1", "id2", "id3", "id4"));
    //
    assertEquals(2, foundRecords.size());
  }

  private void prepareMocksForSearchPublishedDatasetRecords(boolean requestBodyAsNull, String datasetId) {
    String urlString = "/services/metis-datasets/" + UriComponent.contextualEncode(datasetId, UriComponent.Type.PATH_SEGMENT)
        + "/records/published/search";
    var objectMapper = new ObjectMapper();
    JsonNode requestBody = (!requestBodyAsNull ? objectMapper.convertValue(Arrays.asList("id1", "id2", "id3", "id4"),
        JsonNode.class) : null);
    JsonNode responseBody = objectMapper.convertValue(Arrays.asList("id1", "id3"), JsonNode.class);
    new WiremockHelper(wireMockExtension).stubPost(urlString, requestBody, (requestBodyAsNull ? 500 : 200), responseBody);
  }

  private DpsTask prepareDpsTask() {
    DpsTask task = new DpsTask();
    task.setTaskName("oaiPmhHarvestingTask");
    task.setInput(
        OAIPMHHarvestingDetails.builder().repositoryUrl("http://example.com/oai-pmh-repository.xml")
                               .schema("Schema").build());
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
        errorDetails.add(new ErrorDetails(RESOURCE_ID + i, ADDITIONAL_INFORMATION + i));
      }
    }
    return info;
  }
}
