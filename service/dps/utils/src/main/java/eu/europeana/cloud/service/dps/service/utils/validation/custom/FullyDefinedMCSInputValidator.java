package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DatasetRevisionInfo;
import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Verifies if provided {@link DpsTask} has input that is fully defined. Fully defined DatasetRevisionInfo means that all its
 * properties are provided and also Revision is fully defined (provider_id, revisionName and revisionTimestamp)
 */
public class FullyDefinedMCSInputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "Input has to be fully defined. It could be DatasetRevisionInfo with: "
      + "providerId, datasetId, revision name, provider and creationTimestamp has to be provided. "
      + "A revision is fully defined (provider_id, revisionName and revisionTimestamp)"
      + " or instead of DatasetRevisionInfo of BatchInfo with providerId, representationName and batchId filled";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(DpsTask dpsTask) {
    if (dpsTask.getInput() instanceof DatasetRevisionInfo input) {
      return input.getProviderId() != null &&
          input.getDatasetId() != null &&
          input.getRepresentationName() != null &&
          input.getRevision() != null &&
          input.getRevision().getRevisionProviderId() != null &&
          !input.getRevision().getRevisionProviderId().matches("\\s*") &&
          input.getRevision().getRevisionName() != null &&
          !input.getRevision().getRevisionName().matches("\\s*") &&
          input.getRevision().getCreationTimeStamp() != null;
    }
    if (dpsTask.getInput() instanceof BatchInfo input) {
      return input.getProviderId() != null &&
          input.getRepresentationName() != null &&
          input.getBatchId() != null;
    } else {
      return false;
    }
  }
}
