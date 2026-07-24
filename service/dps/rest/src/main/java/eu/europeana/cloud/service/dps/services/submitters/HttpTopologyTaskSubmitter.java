package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.HttpHarvestingDetails;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.http.FileURLCreator;
import eu.europeana.cloud.service.dps.storm.utils.ExpectedCounters;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.HarvestingIterator;
import eu.europeana.metis.harvesting.file.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;

@Service
public class HttpTopologyTaskSubmitter implements TaskSubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpTopologyTaskSubmitter.class);

  private final TaskStatusUpdater taskStatusUpdater;
  private final TaskStatusChecker taskStatusChecker;
  private final RecordSubmitService recordSubmitService;
  private final FileURLCreator fileURLCreator;

  @Value("${harvestingTasksDir}")
  private String harvestingTasksDir;

  public HttpTopologyTaskSubmitter(TaskStatusUpdater taskStatusUpdater,
                                   RecordSubmitService recordSubmitService,
                                   KafkaTopicSelector kafkaTopicSelector,
                                   TaskStatusChecker taskStatusChecker,
                                   FileURLCreator fileURLCreator, DataSetServiceClient dataSetServiceClient) {
    this.taskStatusUpdater = taskStatusUpdater;
    this.recordSubmitService = recordSubmitService;
    this.taskStatusChecker = taskStatusChecker;
    this.fileURLCreator = fileURLCreator;
  }

  @Override
  public ExpectedCounters submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {

    LOGGER.info("HTTP task submission for {} started.", parameters.getTask().getTaskId());

    int expectedCount = -1;
      parameters.getTaskInfo().setExpectedRecords(expectedCount);
    LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);

    try {
      HttpHarvestingDetails input = (HttpHarvestingDetails) parameters.getTask().getSource();
      final String urlToZipFile = input.getRepositoryUrl();
      final HarvestingIterator<Path, Path> iterator = HarvesterFactory.createHttpHarvester()
              .harvestRecords(urlToZipFile,
                      downloadedFileLocationFor(parameters.getTask()));
      expectedCount = iterateOverFiles(iterator, parameters);
      return new ExpectedCounters(expectedCount,0);
    } catch (HarvesterException e) {
      throw new TaskSubmissionException("Submitting http task finished with error!",e);
    }
  }

  private String downloadedFileLocationFor(DpsTask dpsTask) {
    return harvestingTasksDir + File.separator + "task_" + dpsTask.getTaskId();
  }

  private int iterateOverFiles(HarvestingIterator<Path, Path> harvestingIterator,
      SubmitTaskParameters submitTaskParameters) throws HarvesterException {
    int expectedSize = 0;
    try(CloseableIterator<Path> closeableIterator = harvestingIterator.getCloseableIterator()){
      while (closeableIterator.hasNext()){
        Path path = closeableIterator.next();
        if (taskStatusChecker.hasDroppedStatus(submitTaskParameters.getTask().getTaskId())) {
          break;
        }
        DpsRecord dpsRecord;
        try {
          dpsRecord = DpsRecord.builder()
                               .taskId(submitTaskParameters.getTask().getTaskId())
                               .recordId(fileURLCreator.generateUrlFor(path))
                               .build();
        } catch (UnsupportedEncodingException e) {
          taskStatusUpdater.setTaskDropped(submitTaskParameters.getTask().getTaskId(),
              "Unable to generate URL for file: " + path.toString());
          break;
        }
        if (recordSubmitService.submitRecord(dpsRecord, submitTaskParameters)) {
          expectedSize++;
        }
      }
    } catch (IOException e) {
      throw new HarvesterException(e);
    }
    return expectedSize;
  }

}
