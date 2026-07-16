package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.DepublicationInfo;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for validation of depublication tasks
 */
class DpsTaskValidationForDepublicationTopologyTest {

  @Test
  void shouldFailBecauseOfMissingRequiredParametersForDatasetDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.setSource(DepublicationInfo.builder().depublishWholeDataset(true).build());

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailBecauseOfMissingRequiredParametersForRecordsDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.setSource(DepublicationInfo.builder().depublishWholeDataset(true).build());

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailBecauseOfMissingRequiredMetisDatasetIdParameterForDatasetDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.setSource(DepublicationInfo.builder().depublishWholeDataset(true).build());
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK);

    try {
      validator.validate(dpsTask);
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.METIS_DATASET_ID));
    }
  }

  @Test
  void shouldFailBecauseOfMissingRequiredMetisDepublicationReasonParameterForDatasetDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.setSource(DepublicationInfo.builder().depublishWholeDataset(true).build());
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK);

    try {
      validator.validate(dpsTask);
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.DEPUBLICATION_REASON));
    }
  }

  @Test
  void shouldFailBecauseOfMissingRequiredMetisDatasetIdParameterForRecordsDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");
    dpsTask.setSource(DepublicationInfo.builder().europeanaIdsToDepublish(Set.of("records")).build());

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK);

    try {
      validator.validate(dpsTask);
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.METIS_DATASET_ID));
    }
  }

  @Test
  void shouldFailBecauseOfMissingRequiredRecordIdsParameterForRecordsDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK);

    try {
      validator.validate(dpsTask);
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("record"));
    }
  }

  @Test
  void shouldFailBecauseOfMissingRequiredDepublicationReasonForRecordsDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.setSource(DepublicationInfo.builder().europeanaIdsToDepublish(Set.of("records")).build());

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK);

    try {
      validator.validate(dpsTask);
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.DEPUBLICATION_REASON));
    }

  }

  @Test
  void shouldSuccessfullyValidateTheTaskForDatasetDepublication() throws DpsTaskValidationException {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.setSource(DepublicationInfo.builder().depublishWholeDataset(true).build());
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK);

    validator.validate(dpsTask);
  }

  @Test
  void shouldSuccessfullyValidateTheTaskForRecordsDepublication() throws DpsTaskValidationException {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.setSource(DepublicationInfo.builder().europeanaIdsToDepublish(Set.of("metisDS")).build());
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK);

    validator.validate(dpsTask);
  }

}
