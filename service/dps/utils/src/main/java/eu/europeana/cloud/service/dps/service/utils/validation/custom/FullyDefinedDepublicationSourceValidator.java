package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.DepublicationInfo;

/**
 * Verifies if provided {@link CreateDpsTaskRequest} has Depublication source that is fully defined.
 */
public class FullyDefinedDepublicationSourceValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "Depublication input must be whole dataset depublication or must have not empty records list.";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(CreateDpsTaskRequest dpsTask) {
    if (dpsTask.getSource() instanceof DepublicationInfo input) {
      return input.isDepublishWholeDataset() || isNotEmpty(input.getEuropeanaIdsToDepublish());
    } else {
      return false;
    }
  }
}
