package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.DpsTask;
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

    DpsTask dpsTask = new DpsTask();

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailBecauseOfMissingRequiredParametersForRecordsDepublication() {

    DpsTask dpsTask = new DpsTask();

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailBecauseOfMissingRequiredMetisDatasetIdParameterForDatasetDepublication() {

    DpsTask dpsTask = new DpsTask();
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

    DpsTask dpsTask = new DpsTask();
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

    DpsTask dpsTask = new DpsTask();
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

    DpsTask dpsTask = new DpsTask();
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

    DpsTask dpsTask = new DpsTask();
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

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

    validator.validate(dpsTask);
  }

  @Test
  void shouldSuccessfullyValidateTheTaskForRecordsDepublication() throws DpsTaskValidationException {

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    validator.validate(dpsTask);
  }

}
