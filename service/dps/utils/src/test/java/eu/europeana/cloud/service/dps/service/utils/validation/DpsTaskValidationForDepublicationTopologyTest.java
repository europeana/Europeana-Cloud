package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
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

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailBecauseOfMissingRequiredParametersForRecordsDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailBecauseOfMissingRequiredMetisDatasetIdParameterForDatasetDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

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
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

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
    dpsTask.addParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, "records");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

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
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    try {
      validator.validate(dpsTask);
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH));
    }
  }

  @Test
  void shouldFailBecauseOfMissingRequiredDepublicationReasonForRecordsDepublication() {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, "records");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

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
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

    validator.validate(dpsTask);
  }

  @Test
  void shouldSuccessfullyValidateTheTaskForRecordsDepublication() throws DpsTaskValidationException {

    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    validator.validate(dpsTask);
  }

}
