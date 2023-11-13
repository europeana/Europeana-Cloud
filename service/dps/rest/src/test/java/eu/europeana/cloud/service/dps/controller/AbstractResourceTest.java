package eu.europeana.cloud.service.dps.controller;

import static org.mockito.Mockito.reset;

import eu.europeana.cloud.service.dps.ValidationStatisticsService;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.storm.service.ValidationStatisticsServiceImpl;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

public class AbstractResourceTest {

  /* Constants */
  protected static final long TASK_ID = 12345;
  protected static final String TOPOLOGY_NAME = "ANY_TOPOLOGY";
  protected static final String RESULT_RESOURCE_URL = "http://tomcat:8080/mcs/records/ZU5NI2ILYC6RMUZRB53YLIWXPNYFHL5VCX7HE2JCX7OLI2OLIGNQ/representations/DESTINATION-REPRESENTATION/versions/destination_VERSION/files/DESTINATION_FILE";
  protected static final String EMPTY_STRING = "";
  protected static final String TEST_RESOURCE_URL = "http://tomcat:8080/mcs/records/ZU5NI2ILYC6RMUZRB53YLIWXPNYFHL5VCX7HE2JCX7OLI2OLIGNQ/representations/SOURCE-REPRESENTATION/versions/SOURCE_VERSION/files/SOURCE_FILE";

  /* Beans (or mocked beans) */
  protected TopologyManager topologyManager;
  protected ValidationStatisticsService validationStatisticsService;

  /* Main mock for testing MVC in Spring */
  protected MockMvc mockMvc;

  @Autowired
  protected WebApplicationContext applicationContext;

  public AbstractResourceTest() {
  }

  public void init() throws MCSException {
    mockMvc = MockMvcBuilders.webAppContextSetup(applicationContext).build();

    topologyManager = applicationContext.getBean(TopologyManager.class);
    validationStatisticsService = applicationContext.getBean(ValidationStatisticsServiceImpl.class);

    reset(
        topologyManager,
        validationStatisticsService
    );
  }

}
