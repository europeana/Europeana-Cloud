package eu.europeana.cloud.service.dps.services.validators;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.DepublicationInfo;
import eu.europeana.cloud.service.dps.HttpHarvestingDetails;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorFactory;
import org.springframework.stereotype.Service;

import static eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorFactory.DEPUBLICATION_TASK;

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

  public void validateTaskSubmission(CreateDpsTaskRequest task, String topologyName)
      throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException {
    assertContainTopology(topologyName);
    validateTask(task, topologyName);
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

  private void validateTask(CreateDpsTaskRequest task, String topologyName) throws DpsTaskValidationException {
    String taskType = specifyTaskType(task, topologyName);
    DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(taskType);
    validator.validate(task);
  }

  private String specifyTaskType(CreateDpsTaskRequest task, String topologyName) throws DpsTaskValidationException {
    if (task.getSource() instanceof BatchInfo) {
      return topologyName + "_dataset_urls";
    }
    if (task.getSource() instanceof OAIPMHHarvestingDetails || task.getSource() instanceof HttpHarvestingDetails){
      return topologyName + "_repository_urls";
    }

    if(task.getSource() instanceof DepublicationInfo){
      return DEPUBLICATION_TASK;
    }

    throw new DpsTaskValidationException("Validation failed. Missing required input");
  }
}
