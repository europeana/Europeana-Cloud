package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DatasetRevisionInfo;
import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Verifies if provided {@link DpsTask} has MCS output properly defined. Fully defined DatasetRevisionInfo means that all its
 * properties are provided and also Revision is fully defined (provider_id, revisionName and revisionTimestamp).
 *
 */
public class ProperlyDefinedMCSOutputValidator extends CustomValidator {

  public static final String ERROR_MESSAGE = "Output has to be properly defined. It could be DatasetRevisionInfo with: "
      + "providerId, datasetId, revision name, provider and creationTimestamp provided. "
      + " or BatchInfo with providerId, representationName";

  @Override
  public String detailedMessage() {
    return ERROR_MESSAGE;
  }

  @Override
  public boolean test(DpsTask dpsTask) {
    if (dpsTask.getOutput() instanceof DatasetRevisionInfo output) {
      return output.getProviderId() != null &&
          output.getDatasetId() != null &&
          output.getRepresentationName() != null &&
          output.getRevision() != null &&
          output.getRevision().getRevisionProviderId() != null &&
          !output.getRevision().getRevisionProviderId().matches("\\s*") &&
          output.getRevision().getRevisionName() != null &&
          !output.getRevision().getRevisionName().matches("\\s*") &&
          output.getRevision().getCreationTimeStamp() != null;
    }
    if (dpsTask.getOutput() instanceof BatchInfo output) {
      return output.getProviderId() != null &&
          output.getRepresentationName() != null;
    } else {
      return false;
    }
  }
}
