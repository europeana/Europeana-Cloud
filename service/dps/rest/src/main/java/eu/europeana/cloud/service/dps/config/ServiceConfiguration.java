package eu.europeana.cloud.service.dps.config;


import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.http.FileURLCreator;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetStatsRetriever;
import eu.europeana.cloud.service.dps.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.properties.KafkaProperties;
import eu.europeana.cloud.service.dps.properties.MiscProperties;
import eu.europeana.cloud.service.dps.properties.TopologyProperties;
import eu.europeana.cloud.service.dps.properties.ZookeeperProperties;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.services.MetisDatasetService;
import eu.europeana.cloud.service.dps.services.TaskFinishService;
import eu.europeana.cloud.service.dps.services.kafka.RecordKafkaSubmitService;
import eu.europeana.cloud.service.dps.services.postprocessors.HarvestingPostProcessor;
import eu.europeana.cloud.service.dps.services.postprocessors.IndexingPostProcessor;
import eu.europeana.cloud.service.dps.services.postprocessors.PostProcessingScheduler;
import eu.europeana.cloud.service.dps.services.postprocessors.PostProcessingService;
import eu.europeana.cloud.service.dps.services.postprocessors.PostProcessorFactory;
import eu.europeana.cloud.service.dps.services.submitters.MCSTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraAttributeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.GeneralStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.StatisticsReportDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.service.TaskExecutionReportServiceImpl;
import eu.europeana.cloud.service.dps.storm.service.ValidationStatisticsServiceImpl;
import eu.europeana.cloud.service.dps.storm.utils.RecordStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.web.common.LoggingFilter;
import java.util.Arrays;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableAsync
@EnableScheduling
@PropertySource("classpath:dps.properties")
public class ServiceConfiguration implements WebMvcConfigurer, AsyncConfigurer {

  private ZookeeperProperties zookeeperProperties;
  private KafkaProperties kafkaProperties;
  private CassandraProperties cassandraProperties;
  private MiscProperties miscProperties;
  private TopologyProperties topologyProperties;


  @Autowired
  public ServiceConfiguration(ZookeeperProperties zookeeperProperties,
      KafkaProperties kafkaProperties,
      CassandraProperties cassandraProperties,
      MiscProperties miscProperties,
      TopologyProperties topologyProperties) {
    this.kafkaProperties = kafkaProperties;
    this.cassandraProperties = cassandraProperties;
    this.zookeeperProperties = zookeeperProperties;
    this.miscProperties = miscProperties;
    this.topologyProperties = topologyProperties;
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(new LoggingFilter());
  }

  @Bean
  public DispatcherServlet dispatcherServlet() {
    return new DispatcherServlet();
  }

  @Bean
  public DispatcherServletRegistrationBean dispatcherServletRegistration() {
    DispatcherServletRegistrationBean registration = new DispatcherServletRegistrationBean(dispatcherServlet(), "/services/");
    registration.setName(DispatcherServletAutoConfiguration.DEFAULT_DISPATCHER_SERVLET_REGISTRATION_BEAN_NAME);
    return registration;
  }

  @Bean
  public MethodValidationPostProcessor methodValidationPostProcessor() {
    return new MethodValidationPostProcessor();
  }

  @Bean
  public RecordExecutionSubmitService recordKafkaSubmitService() {
    return new RecordKafkaSubmitService(kafkaProperties.getBrokerLocation());
  }

  @Bean
  public RecordSubmitService recordSubmitService() {
    return new RecordSubmitService(processedRecordsDAO(), recordKafkaSubmitService());
  }

  @Bean
  public TaskExecutionReportServiceImpl taskReportService() {
    return new TaskExecutionReportServiceImpl(subTaskInfoDAO(), taskErrorDAO(), taskInfoDAO());
  }

  @Bean
  public TopologyManager topologyManger() {
    return new TopologyManager(topologyProperties.getNameList());
  }

  @Bean
  public CassandraConnectionProvider dpsCassandraProvider() {
    return new CassandraConnectionProvider(
        cassandraProperties.getHosts(),
        Integer.parseInt(cassandraProperties.getPort()),
        cassandraProperties.getDPSKeyspace(),
        cassandraProperties.getDPSUser(),
        cassandraProperties.getDPSPassword());
  }

  @Bean
  public CassandraConnectionProvider aasCassandraProvider() {
    return new CassandraConnectionProvider(
        cassandraProperties.getHosts(),
        Integer.parseInt(cassandraProperties.getPort()),
        cassandraProperties.getAASKeyspace(),
        cassandraProperties.getAASUser(),
        cassandraProperties.getAASPassword()
    );
  }

  @Bean

  public String applicationIdentifier() {
    return miscProperties.getAppId();
  }

  @Bean
  public MethodInvokingFactoryBean methodInvokingFactoryBean() {
    var result = new MethodInvokingFactoryBean();
    result.setTargetClass(SecurityContextHolder.class);
    result.setTargetMethod("setStrategyName");
    result.setArguments("MODE_INHERITABLETHREADLOCAL");
    return result;
  }

  @Bean
  public CassandraTaskInfoDAO taskInfoDAO() {
    return new CassandraTaskInfoDAO(dpsCassandraProvider());
  }

  @Bean
  public TaskDiagnosticInfoDAO taskDiagnosticInfoDAO() {
    return new TaskDiagnosticInfoDAO(dpsCassandraProvider());
  }

  @Bean
  public HarvestedRecordsDAO harvestedRecordsDAO() {
    return new HarvestedRecordsDAO(dpsCassandraProvider());
  }


  @Bean
  public CassandraTaskErrorsDAO taskErrorDAO() {
    return new CassandraTaskErrorsDAO(dpsCassandraProvider());
  }

  @Bean
  public TasksByStateDAO tasksByStateDAO() {
    return new TasksByStateDAO(dpsCassandraProvider());
  }

  @Bean
  public ValidationStatisticsServiceImpl validationStatisticsService() {
    return new ValidationStatisticsServiceImpl(
        cassandraGeneralStatisticsDAO(),
        cassandraNodeStatisticsDAO(),
        cassandraAttributeStatisticsDAO(),
        cassandraStatisticsReportDAO());
  }

  @Bean
  public GeneralStatisticsDAO cassandraGeneralStatisticsDAO() {
    return new GeneralStatisticsDAO(dpsCassandraProvider());
  }

  @Bean
  public CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO() {
    return new CassandraNodeStatisticsDAO(dpsCassandraProvider());
  }

  @Bean
  public CassandraAttributeStatisticsDAO cassandraAttributeStatisticsDAO() {
    return new CassandraAttributeStatisticsDAO(dpsCassandraProvider());
  }

  @Bean
  public StatisticsReportDAO cassandraStatisticsReportDAO() {
    return new StatisticsReportDAO(dpsCassandraProvider());
  }

  @Bean
  public TaskStatusChecker taskStatusChecker() {
    return new TaskStatusChecker(taskInfoDAO());
  }

  @Bean
  public NotificationsDAO subTaskInfoDAO() {
    return new NotificationsDAO(dpsCassandraProvider());
  }

  @Bean
  public ProcessedRecordsDAO processedRecordsDAO() {
    return new ProcessedRecordsDAO(dpsCassandraProvider());
  }

  @Bean
  public TaskStatusUpdater taskStatusUpdater() {
    return new TaskStatusUpdater(taskInfoDAO(), tasksByStateDAO(), applicationIdentifier());
  }

  @Bean
  public TaskStatusSynchronizer taskStatusSynchronizer() {
    return new TaskStatusSynchronizer(taskInfoDAO(), tasksByStateDAO(), taskStatusUpdater());
  }

  @Bean
  public RecordStatusUpdater recordStatusUpdater(NotificationsDAO cassandraSubTaskInfoDAO) {
    return new RecordStatusUpdater(cassandraSubTaskInfoDAO);
  }

  @Bean
  public MCSTaskSubmitter mcsTaskSubmitter() {
    return new MCSTaskSubmitter(taskStatusChecker(), taskStatusUpdater(), recordSubmitService(), mcsLocation(),
        topologyProperties.getUser(),
        topologyProperties.getPassword());
  }

  @Bean
  public FileURLCreator fileURLCreator() {
    String machineLocation = miscProperties.getMachineLocation();
    if (machineLocation == null) {
      throw new BeanCreationException(
          String.format("Property 'misc.machineLocation' must be set in properties file"));
    }
    return new FileURLCreator(machineLocation);
  }

  @Bean
  public PostProcessorFactory postProcessorFactory() {
    return new PostProcessorFactory(
        Arrays.asList(harvestingPostProcessor(), indexingPostProcessor())
    );
  }

  @Bean
  public HarvestingPostProcessor harvestingPostProcessor() {
    return new HarvestingPostProcessor(harvestedRecordsDAO(), processedRecordsDAO(),
        recordServiceClient(), revisionServiceClient(), uisClient(), taskStatusUpdater(),
        taskStatusChecker());
  }

  @Bean
  public IndexingPostProcessor indexingPostProcessor() {
    return new IndexingPostProcessor(taskStatusUpdater(), harvestedRecordsDAO(), taskStatusChecker(), indexWrapper());
  }

  @Bean
  public UISClient uisClient() {
    return new UISClient(
        uisLocation(),
        topologyProperties.getUser(),
        topologyProperties.getPassword());
  }

  @Bean
  public DataSetServiceClient dataSetServiceClient() {
    return new DataSetServiceClient(
        mcsLocation(),
        topologyProperties.getUser(),
        topologyProperties.getPassword());
  }

  @Bean
  public RecordServiceClient recordServiceClient() {
    return new RecordServiceClient(
        mcsLocation(),
        topologyProperties.getUser(),
        topologyProperties.getPassword());
  }

  @Bean
  public RevisionServiceClient revisionServiceClient() {
    return new RevisionServiceClient(
        mcsLocation(),
        topologyProperties.getUser(),
        topologyProperties.getPassword());
  }

  @Bean
  public RetryAspect retryAspect() {
    return new RetryAspect();
  }

  @Bean
  public PostProcessingService postProcessingService() {
    return new PostProcessingService(
        postProcessorFactory(),
        taskInfoDAO(),
        taskDiagnosticInfoDAO(),
        taskStatusUpdater());
  }

  @Bean
  public TaskFinishService taskFinishService(PostProcessingService postProcessingService,
      TasksByStateDAO tasksByStateDAO,
      CassandraTaskInfoDAO taskInfoDAO,
      TaskStatusUpdater taskStatusUpdater,
      String applicationIdentifier
  ) {
    return new TaskFinishService(postProcessingService, tasksByStateDAO, taskInfoDAO, taskStatusUpdater, applicationIdentifier);
  }

  @Bean
  public PostProcessingScheduler postProcessingScheduler(
      PostProcessingService postProcessingService,
      TasksByStateDAO tasksByStateDAO,
      TaskStatusUpdater taskStatusUpdater,
      String applicationIdentifier
  ) {
    return new PostProcessingScheduler(postProcessingService, tasksByStateDAO, taskStatusUpdater, applicationIdentifier);
  }

  @Bean
  public MetisDatasetService metisDatasetService(DatasetStatsRetriever datasetStatsRetriever,
      HarvestedRecordsDAO harvestedRecordsDAO) {
    return new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
  }

  @Bean
  public DatasetStatsRetriever datasetStatsRetriever(IndexWrapper indexWrapper) {
    return new DatasetStatsRetriever(indexWrapper);
  }

  @Bean
  public IndexWrapper indexWrapper() {
    return new IndexWrapper();
  }

  @Bean
  @Override
  public AsyncTaskExecutor getAsyncExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(40);
    executor.setMaxPoolSize(40);
    executor.setQueueCapacity(10);
    executor.setThreadNamePrefix("DPSThreadPool-");
    return executor;
  }

  @Bean("postProcessingExecutor")
  public AsyncTaskExecutor postProcessingExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(16);
    executor.setMaxPoolSize(16);
    executor.setQueueCapacity(10);
    executor.setThreadNamePrefix("post-proprocessing-");
    return executor;
  }

  private String mcsLocation() {
    return miscProperties.getMcsLocation();
  }

  private String uisLocation() {
    return miscProperties.getUisLocation();
  }
}
