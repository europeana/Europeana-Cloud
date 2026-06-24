package eu.europeana.cloud.service.dps.services.validators;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.FilesUrls;
import eu.europeana.cloud.service.dps.HttpHarvestingDetails;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorFactory;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import org.springframework.stereotype.Service;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.METIS_DATASET_ID;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH;

/**
 * This service will be used during submission time to validate if given task submission is correct.<br/> For now we are checking
 * this constraints:
 * <li>if task submission is done for the topology that exists (that is handled by this DPS application)</li>
 * <li>if the task is valid</li>
 * <li>if the datasets specified in the submission request exists in the ECloud</li>
 */
@Service
public class TaskSubmissionValidator {
  private final TopologyManager topologyManager;

  public TaskSubmissionValidator(TopologyManager topologyManager) {
    this.topologyManager = topologyManager;
  }

  public void validateTaskSubmission(SubmitTaskParameters parameters)
      throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException {
    assertContainTopology(parameters.getTaskInfo().getTopologyName());
    validateTask(parameters.getTask(), parameters.getTaskInfo().getTopologyName());
  }

  /**
   * Checks if provided topology name is on the list of all the topologies that are handled by the DPS application
   *
   * @param topology topologyName to be checked
   * @throws AccessDeniedOrTopologyDoesNotExistException thrown in case there is no provided topology name on the list of all
   * supported topologies
   */
  public void assertContainTopology(String topology) throws AccessDeniedOrTopologyDoesNotExistException {
    if (!topologyManager.containsTopology(topology)) {
      throw new AccessDeniedOrTopologyDoesNotExistException("The topology doesn't exist");
    }
  }

  private void validateTask(DpsTask task, String topologyName) throws DpsTaskValidationException {
    String taskType = specifyTaskType(task, topologyName);
    DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(taskType);
    validator.validate(task);
  }

  private String specifyTaskType(DpsTask task, String topologyName) throws DpsTaskValidationException {
    if (task.getInput() instanceof FilesUrls) {
      return topologyName + "_file_urls";
    }
    if (task.getInput() instanceof BatchInfo) {
      return topologyName + "_dataset_urls";
    }
    if (task.getInput() instanceof OAIPMHHarvestingDetails || task.getInput() instanceof HttpHarvestingDetails){
      return topologyName + "_repository_urls";
    }
    if (task.getParameter(METIS_DATASET_ID) != null) {
      return topologyName + "_" + METIS_DATASET_ID.toLowerCase();
    }
    if (task.getParameter(RECORD_IDS_TO_DEPUBLISH) != null) {
      return topologyName + "_" + RECORD_IDS_TO_DEPUBLISH.toLowerCase();
    }
    throw new DpsTaskValidationException("Validation failed. Missing required input");
  }
}
