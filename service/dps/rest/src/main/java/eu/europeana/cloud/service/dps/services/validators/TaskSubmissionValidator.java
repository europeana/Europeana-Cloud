package eu.europeana.cloud.service.dps.services.validators;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.METIS_DATASET_ID;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorFactory;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * This service will be used during submission time to validate if given task submission is correct.<br/> For now we are checking
 * this constraints:
 * <li>if task submission is done for the topology that exists (that is handled by this DPS application)</li>
 * <li>if the task is valid</li>
 * <li>if the datasets specified in the submission request exists in the ECloud</li>
 */
@Service
public class TaskSubmissionValidator {

  private final DataSetServiceClient dataSetServiceClient;
  private final TopologyManager topologyManager;

  public TaskSubmissionValidator(DataSetServiceClient dataSetServiceClient, TopologyManager topologyManager) {
    this.dataSetServiceClient = dataSetServiceClient;
    this.topologyManager = topologyManager;
  }

  public void validateTaskSubmission(SubmitTaskParameters parameters)
      throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException {
    assertContainTopology(parameters.getTaskInfo().getTopologyName());
    validateTask(parameters.getTask(), parameters.getTaskInfo().getTopologyName());
    validateOutputDataSets(parameters.getTask());
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

  private void validateOutputDataSets(DpsTask task) throws DpsTaskValidationException {
    List<String> dataSets = readDataSetsList(task.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS));
    for (String dataSetURL : dataSets) {
      try {
        DataSet dataSet = DataSetUrlParser.parse(dataSetURL);
        if( !dataSetServiceClient.datasetExists(dataSet.getProviderId(), dataSet.getId())){
          throw new DataSetNotExistsException();
        }
        validateProviderId(task, dataSet.getProviderId());
      } catch (MalformedURLException e) {
        throw new DpsTaskValidationException("Validation failed. This output dataSet " + dataSetURL
            + " can not be submitted because: " + e.getMessage(), e);
      } catch (DataSetNotExistsException e) {
        throw new DpsTaskValidationException("Validation failed. This output dataSet " + dataSetURL
            + " Does not exist", e);
      } catch (Exception e) {
        throw new DpsTaskValidationException("Unexpected exception happened while validating the dataSet: "
            + dataSetURL + " because of: " + e.getMessage(), e);
      }
    }
  }

  private void validateProviderId(DpsTask task, String providerId) throws DpsTaskValidationException {
    String providedProviderId = task.getParameter(PluginParameterKeys.PROVIDER_ID);
    if (providedProviderId != null && !providedProviderId.equals(providerId)) {
      throw new DpsTaskValidationException("Validation failed. The provider id: " + providedProviderId
          + " should be the same provider of the output dataSet: " + providerId);
    }
  }

  private List<String> readDataSetsList(String listParameter) {
    return listParameter == null ?
        Arrays.asList() :
        Arrays.asList(listParameter.split(","));
  }

  private String specifyTaskType(DpsTask task, String topologyName) throws DpsTaskValidationException {
    if (task.getDataEntry(FILE_URLS) != null) {
      return topologyName + "_" + FILE_URLS.name().toLowerCase();
    }
    if (task.getDataEntry(DATASET_URLS) != null) {
      return topologyName + "_" + DATASET_URLS.name().toLowerCase();
    }
    if (task.getDataEntry(REPOSITORY_URLS) != null) {
      return topologyName + "_" + REPOSITORY_URLS.name().toLowerCase();
    }
    if (task.getParameter(METIS_DATASET_ID) != null) {
      return topologyName + "_" + METIS_DATASET_ID.toLowerCase();
    }
    if (task.getParameter(RECORD_IDS_TO_DEPUBLISH) != null) {
      return topologyName + "_" + RECORD_IDS_TO_DEPUBLISH.toLowerCase();
    }
    throw new DpsTaskValidationException("Validation failed. Missing required data_entry");
  }
}
