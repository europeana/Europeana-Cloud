package eu.europeana.cloud.service.dps.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.util.NestedServletException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {SpiedDpsTestContext.class, ReportResource.class})
@WebAppConfiguration
@TestPropertySource(properties = {"numberOfElementsOnPage=100","maxIdentifiersCount=100"})
public class ReportResourceTest extends AbstractResourceTest {

    /* Endpoints */
    private static final String WEB_TARGET = ReportResource.class.getAnnotation(RequestMapping.class).value()[0];
    private static final String DETAILED_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/reports/details";
    private static final String ERRORS_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/reports/errors";
    private static final String VALIDATION_STATISTICS_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/statistics";
    private static final String ELEMENT_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/reports/element";

    /* Constants */
    private static final String ERROR_MESSAGE = "Message";
    private static final String[] ERROR_TYPES = {"bd0c7280-db47-11e7-ada4-e2f54b49d956", "bd0ac4d0-db47-11e7-ada4-e2f54b49d956", "4bb74640-db48-11e7-af3d-e2f54b49d956"};
    private static final int[] ERROR_COUNTS = {5, 2, 7};
    private static final String ERROR_RESOURCE_IDENTIFIER = "Resource id ";
    private static final String ADDITIONAL_INFORMATIONS = "Additional informations ";
    public static final String PATH = "path";
    public static final String PATH_VALUE = "ELEMENT";

    /* Beans (or mocked beans) */
    private TaskExecutionReportService reportService;

    public ReportResourceTest() {
        super();
    }

    @Before
    public void init() {
        super.init();
        reportService = applicationContext.getBean(TaskExecutionReportService.class);
        reset(reportService);
    }

    @Test
    public void shouldGetDetailedReportForTheFirst100Resources() throws Exception {
        List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        when(reportService.getDetailedTaskReportBetweenChunks(eq(Long.toString(TASK_ID)), eq(1), eq(100))).thenReturn(subTaskInfoList);

        ResultActions response = mockMvc.perform(get(DETAILED_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID));

        assertDetailedReportResponse(subTaskInfoList.get(0), response);
    }

    @Test
    public void shouldThrowExceptionWhenTaskDoesNotBelongToTopology() throws Exception {
        List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        when(reportService.getDetailedTaskReportBetweenChunks(eq(Long.toString(TASK_ID)), eq(1), eq(100))).thenReturn(subTaskInfoList);
        try {
            ResultActions response = mockMvc.perform(
                    get(DETAILED_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
            );
        } catch (NestedServletException nse) {
            assertSame(AccessDeniedOrObjectDoesNotExistException.class, nse.getCause().getClass());
        }
    }

    @Test
    public void shouldGetDetailedReportForSpecifiedResources() throws Exception {
        List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
        when(reportService.getDetailedTaskReportBetweenChunks(eq(Long.toString(TASK_ID)), eq(120), eq(150))).thenReturn(subTaskInfoList);
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        ResultActions response = mockMvc.perform(get(DETAILED_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID).queryParam("from", "120").queryParam("to", "150"));
        assertDetailedReportResponse(subTaskInfoList.get(0), response);
    }

    @Test
    public void shouldGetGeneralErrorReportWithIdentifiers() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        TaskErrorsInfo errorsInfo = createDummyErrorsInfo(true);
        when(reportService.getGeneralTaskErrorReport(eq(Long.toString(TASK_ID)), eq(10))).thenReturn(errorsInfo);

        ResultActions response = mockMvc.perform(
                get(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
                        .queryParam("error", "null")
                        .queryParam("idsCount", "10")
        );
        TaskErrorsInfo retrievedInfo = new ObjectMapper().readValue(
                response.andReturn().getResponse().getContentAsString(),TaskErrorsInfo.class);
        assertThat(retrievedInfo, is(errorsInfo));
    }


    @Test
    public void shouldCheckIfReportExists() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(reportService.checkIfReportExists(eq(Long.toString(TASK_ID)))).thenReturn(true);
        ResultActions response = mockMvc.perform(head(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID));
        response.andExpect(status().isOk());
    }

    @Test
    public void shouldReturn405InCaseOfException() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService).checkIfReportExists(eq(Long.toString(TASK_ID)));
        try {
            ResultActions response = mockMvc.perform(
                    head(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
            );
        }catch(NestedServletException nse){
            assertSame(AccessDeniedOrObjectDoesNotExistException.class, nse.getCause().getClass());
        }
    }

    @Test
    public void shouldGetSpecificErrorReport() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        TaskErrorsInfo errorsInfo = createDummyErrorsInfo(true);
        when(reportService.getSpecificTaskErrorReport(eq(Long.toString(TASK_ID)), eq(ERROR_TYPES[0]), eq(100))).thenReturn(errorsInfo);

        ResultActions response = mockMvc.perform(get(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID).queryParam("error", ERROR_TYPES[0]));
        TaskErrorsInfo retrievedInfo = new ObjectMapper().readValue(response.andReturn().getResponse().getContentAsString(),TaskErrorsInfo.class);
        assertThat(retrievedInfo, is(errorsInfo));
    }


    @Test
    public void shouldGetGeneralErrorReport() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        TaskErrorsInfo errorsInfo = createDummyErrorsInfo(false);
        when(reportService.getGeneralTaskErrorReport(eq(Long.toString(TASK_ID)), eq(0))).thenReturn(errorsInfo);

        ResultActions response = mockMvc.perform(
                get(ERRORS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
                        .queryParam("error", "null")
        );

        TaskErrorsInfo retrievedInfo = new ObjectMapper().readValue(
                response.andReturn().getResponse().getContentAsString(),TaskErrorsInfo.class);
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
                .andExpect(jsonPath("taskId", org.hamcrest.Matchers.is((int)TASK_ID)));

    }

    @Test
    public void shouldReturn405WhenStatisticsRequestedButTopologyNotFound() throws Exception {
        when(validationStatisticsService.getTaskStatisticsReport(TASK_ID)).thenReturn(new StatisticsReport(TASK_ID, null));
        when(topologyManager.containsTopology(anyString())).thenReturn(false);
        try {
            ResultActions response = mockMvc.perform(
                    get(VALIDATION_STATISTICS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
            );
        }catch (NestedServletException nse) {
            assertSame(AccessDeniedOrTopologyDoesNotExistException.class, nse.getCause().getClass());
        }
    }

    @Test
    public void shouldGetElementReport() throws Exception {
        NodeReport nodeReport = new NodeReport("VALUE", 5, Arrays.asList(new AttributeStatistics("Attr1", "Value1", 10)));
        when(validationStatisticsService.getElementReport(TASK_ID, PATH_VALUE)).thenReturn(Arrays.asList(nodeReport));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        ResultActions response = mockMvc.perform(get(ELEMENT_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID).queryParam(PATH, PATH_VALUE));

        response.andExpect(status().isOk());
    }


    /////////////////////
    private List<SubTaskInfo> createDummySubTaskInfoList() {
        List<SubTaskInfo> subTaskInfoList = new ArrayList<>();
        SubTaskInfo subTaskInfo = new SubTaskInfo(1, TEST_RESOURCE_URL, RecordState.SUCCESS, EMPTY_STRING, EMPTY_STRING, RESULT_RESOURCE_URL);
        subTaskInfoList.add(subTaskInfo);
        return subTaskInfoList;
    }

    private void assertDetailedReportResponse(SubTaskInfo subTaskInfo, ResultActions detailedReportResponse) throws Exception {
        detailedReportResponse.andExpect(status().isOk());
        String resultString = detailedReportResponse.andReturn().getResponse().getContentAsString();

        List<SubTaskInfo> resultedSubTaskInfoList = new ObjectMapper().readValue(resultString,new TypeReference<List<SubTaskInfo>>() {
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
                    errorDetails.add(new ErrorDetails(ERROR_RESOURCE_IDENTIFIER + j, ADDITIONAL_INFORMATIONS + j));
                }
            }
            errors.add(error);
        }
        return info;
    }



}
