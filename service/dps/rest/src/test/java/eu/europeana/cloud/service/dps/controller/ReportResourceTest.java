package eu.europeana.cloud.service.dps.controller;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.ErrorDetails;
import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.config.DPSServiceTestContext;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.services.submitters.MCSTaskSubmitter;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.bind.annotation.RequestMapping;

@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {DPSServiceTestContext.class, ReportResource.class, TaskStatusUpdater.class,
    MCSTaskSubmitter.class})
@TestPropertySource(properties = {"numberOfElementsOnPage=100", "maxIdentifiersCount=100"})
public class ReportResourceTest extends AbstractResourceTest {

  /* Endpoints */
  private final static String WEB_TARGET = ReportResource.class.getAnnotation(RequestMapping.class).value()[0];
  private final static String DETAILED_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/reports/details";
  private final static String ERRORS_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/reports/errors";
  private final static String VALIDATION_STATISTICS_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/statistics";
  private final static String ELEMENT_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/reports/element";

  /* Constants */
  private final static String ERROR_MESSAGE = "Message";
  private final static String[] ERROR_TYPES = {"bd0c7280-db47-11e7-ada4-e2f54b49d956", "bd0ac4d0-db47-11e7-ada4-e2f54b49d956",
      "4bb74640-db48-11e7-af3d-e2f54b49d956"};
  private final static int[] ERROR_COUNTS = {5, 2, 7};
  private final static String ERROR_RESOURCE_IDENTIFIER = "Resource id ";
  private final static String ADDITIONAL_INFORMATION = "Additional information";
  public final static String PATH = "path";
  public final static String PATH_VALUE = "ELEMENT";

  /* Beans (or mocked beans) */
  private TaskExecutionReportService reportService;

  public ReportResourceTest() {
    super();
  }

  @Before
  public void init() throws MCSException {
    super.init();
    reportService = applicationContext.getBean(TaskExecutionReportService.class);
    reset(reportService);
  }

  @Test
  public void shouldGetDetailedReportForTheFirst100Resources() throws Exception {
    List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
    doNothing().when(reportService).checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);
    when(topologyManager.containsTopology(anyString())).thenReturn(true);
    when(reportService.getDetailedTaskReport(TASK_ID, 1, 100)).thenReturn(subTaskInfoList);

    ResultActions response = mockMvc.perform(get(DETAILED_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID));

    assertDetailedReportResponse(subTaskInfoList.get(0), response);
  }

  @Test
  public void shouldThrowExceptionWhenTaskDoesNotBelongToTopology() throws Exception {
    List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
    doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService)
                                                            .checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);
    when(topologyManager.containsTopology(anyString())).thenReturn(true);
    when(reportService.getDetailedTaskReport(TASK_ID, 1, 100)).thenReturn(subTaskInfoList);

    ResultActions response = mockMvc.perform(
        get(DETAILED_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
    );
    response.andExpect(status().isMethodNotAllowed());
  }


  @Test
  public void shouldGetDetailedReportForSpecifiedResources() throws Exception {
    List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
    when(reportService.getDetailedTaskReport(TASK_ID, 120, 150)).thenReturn(subTaskInfoList);
    when(topologyManager.containsTopology(anyString())).thenReturn(true);
    doNothing().when(reportService).checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);
    ResultActions response = mockMvc.perform(
        get(DETAILED_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID).queryParam("from", "120").queryParam("to", "150"));
    assertDetailedReportResponse(subTaskInfoList.get(0), response);
  }


  @Test
  public void shouldGetGeneralErrorReportWithIdentifiers() throws Exception {
    when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
    TaskErrorsInfo errorsInfo = createDummyErrorsInfo(true);
    when(reportService.getGeneralTaskErrorReport(TASK_ID, 10)).thenReturn(errorsInfo);

    ResultActions response = mockMvc.perform(
        get(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
            .queryParam("error", "null")
            .queryParam("idsCount", "10")
    );
    TaskErrorsInfo retrievedInfo = new ObjectMapper().readValue(
        response.andReturn().getResponse().getContentAsString(), TaskErrorsInfo.class);
    assertThat(retrievedInfo, is(errorsInfo));
  }


  @Test
  public void shouldCheckIfReportExists() throws Exception {
    when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
    doNothing().when(reportService).checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);
    when(reportService.checkIfReportExists(TASK_ID)).thenReturn(true);
    ResultActions response = mockMvc.perform(head(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID));
    response.andExpect(status().isOk());
  }


  @Test
  public void shouldReturn405InCaseOfException() throws Exception {
    when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
    doNothing().when(reportService).checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);
    when(reportService.checkIfReportExists(TASK_ID)).thenReturn(false);

    ResultActions response = mockMvc.perform(
        head(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
    );
    response.andExpect(status().isMethodNotAllowed());
  }


  @Test
  public void shouldGetSpecificErrorReport() throws Exception {
    when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
    TaskErrorsInfo errorsInfo = createDummyErrorsInfo(true);
    when(reportService.getSpecificTaskErrorReport(TASK_ID, ERROR_TYPES[0], 100)).thenReturn(errorsInfo);

    ResultActions response = mockMvc.perform(
        get(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID).queryParam("error", ERROR_TYPES[0]));
    TaskErrorsInfo retrievedInfo = new ObjectMapper().readValue(response.andReturn().getResponse().getContentAsString(),
        TaskErrorsInfo.class);
    assertThat(retrievedInfo, is(errorsInfo));
  }


  @Test
  public void shouldGetGeneralErrorReport() throws Exception {
    when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
    TaskErrorsInfo errorsInfo = createDummyErrorsInfo(false);
    when(reportService.getGeneralTaskErrorReport(TASK_ID, 0)).thenReturn(errorsInfo);

    ResultActions response = mockMvc.perform(
        get(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
            .queryParam("error", "null")
    );

    TaskErrorsInfo retrievedInfo = new ObjectMapper().readValue(
        response.andReturn().getResponse().getContentAsString(), TaskErrorsInfo.class);
    assertThat(retrievedInfo, is(errorsInfo));
  }


  @Test
  public void shouldGetStatisticReport() throws Exception {
    when(validationStatisticsService.getTaskStatisticsReport(TASK_ID)).thenReturn(new StatisticsReport(TASK_ID, null));
    when(topologyManager.containsTopology(anyString())).thenReturn(true);
    ResultActions response = mockMvc.perform(get(VALIDATION_STATISTICS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID));

    System.err.println(content().string("taskId"));

    response
        .andExpect(status().isOk())
        .andExpect(jsonPath("taskId", org.hamcrest.Matchers.is((int) TASK_ID)));

  }


  @Test
  public void shouldReturn405WhenStatisticsRequestedButTopologyNotFound() throws Exception {
    when(validationStatisticsService.getTaskStatisticsReport(TASK_ID)).thenReturn(new StatisticsReport(TASK_ID, null));
    when(topologyManager.containsTopology(anyString())).thenReturn(false);

    ResultActions response = mockMvc.perform(
        get(VALIDATION_STATISTICS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
    );
    response.andExpect(status().isMethodNotAllowed());
  }


  @Test
  public void shouldGetElementReport() throws Exception {
    NodeReport nodeReport = new NodeReport("VALUE", 5, Collections.singletonList(new AttributeStatistics("Attr1", "Value1", 10)));
    when(validationStatisticsService.getElementReport(TASK_ID, PATH_VALUE)).thenReturn(Collections.singletonList(nodeReport));
    when(topologyManager.containsTopology(anyString())).thenReturn(true);
    ResultActions response = mockMvc.perform(get(ELEMENT_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID).queryParam(PATH, PATH_VALUE));

    response.andExpect(status().isOk());
  }


  /* Utilities */

  private List<SubTaskInfo> createDummySubTaskInfoList() {
    List<SubTaskInfo> subTaskInfoList = new ArrayList<>();
    SubTaskInfo subTaskInfo = new SubTaskInfo(1, TEST_RESOURCE_URL, RecordState.SUCCESS, EMPTY_STRING, EMPTY_STRING,
        RESULT_RESOURCE_URL, 0L);
    subTaskInfoList.add(subTaskInfo);
    return subTaskInfoList;
  }

  private void assertDetailedReportResponse(SubTaskInfo subTaskInfo, ResultActions detailedReportResponse) throws Exception {
    detailedReportResponse.andExpect(status().isOk());
    String resultString = detailedReportResponse.andReturn().getResponse().getContentAsString();

    List<SubTaskInfo> resultedSubTaskInfoList = new ObjectMapper().readValue(
        resultString, new TypeReference<>() {
        });
    assertThat(resultedSubTaskInfoList.get(0), is(subTaskInfo));
  }

  private TaskErrorsInfo createDummyErrorsInfo(boolean specific) {
    TaskErrorsInfo info = new TaskErrorsInfo(TASK_ID);
    List<TaskErrorInfo> errors = new ArrayList<>();
    info.setErrors(errors);
    for (int i = 0; i < 3; i++) {
      TaskErrorInfo error = new TaskErrorInfo();
      error.setMessage(ERROR_MESSAGE);
      error.setErrorType(ERROR_TYPES[i]);
      error.setOccurrences(ERROR_COUNTS[i]);
      if (specific) {
        List<ErrorDetails> errorDetails = new ArrayList<>();
        error.setErrorDetails(errorDetails);
        for (int j = 0; j < ERROR_COUNTS[i]; j++) {
          errorDetails.add(new ErrorDetails(ERROR_RESOURCE_IDENTIFIER + j, ADDITIONAL_INFORMATION + j));
        }
      }
      errors.add(error);
    }
    return info;
  }

}
