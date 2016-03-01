package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.ApplicationContextUtils;
import eu.europeana.cloud.service.dps.TaskExecutionKillService;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.TaskExecutionSubmitService;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.model.MutableAclService;

import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;


public class TopologyTasksResourceTest extends JerseyTest {
    private TaskExecutionReportService reportService;
    private TaskExecutionSubmitService submitService;
    private TaskExecutionKillService killService;
    private TopologyManager topologyManager;
    private MutableAclService mutableAclService;
    private String mcsLocation;
    private RecordServiceClient recordServiceClient;
    private WebTarget webTarget;

    @Override
    protected Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedDpsTestContext.xml");
    }

    @Before
    public void init() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        reportService = applicationContext.getBean(TaskExecutionReportService.class);
        submitService = applicationContext.getBean(TaskExecutionSubmitService.class);
        killService = applicationContext.getBean(TaskExecutionKillService.class);
        topologyManager = applicationContext.getBean(TopologyManager.class);
        mutableAclService = applicationContext.getBean(MutableAclService.class);
        mcsLocation = applicationContext.getBean(String.class);
        recordServiceClient = applicationContext.getBean(RecordServiceClient.class);
        webTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value());
    }

    @Test
    public void shouldSendRequestCorrectly() {
       assertThat(reportService,notNullValue());
    }



}