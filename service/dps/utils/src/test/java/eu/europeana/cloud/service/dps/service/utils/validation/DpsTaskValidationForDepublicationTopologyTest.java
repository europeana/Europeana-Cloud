package eu.europeana.cloud.service.dps.service.utils.validation;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for validation of depublication tasks
 */
public class DpsTaskValidationForDepublicationTopologyTest {

  @Test(expected = DpsTaskValidationException.class)
  public void shouldFailBecauseOfMissingRequiredParametersForDatasetDepublication() throws DpsTaskValidationException {

    DpsTask dpsTask = new DpsTask();

    DpsTaskValidator validator =
        DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

    validator.validate(dpsTask);
  }

  @Test(expected = DpsTaskValidationException.class)
  public void shouldFailBecauseOfMissingRequiredParametersForRecordsDepublication() throws DpsTaskValidationException {

    DpsTask dpsTask = new DpsTask();

    DpsTaskValidator validator =
        DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    validator.validate(dpsTask);
  }

  @Test
  public void shouldFailBecauseOfMissingRequiredMetisDatasetIdParameterForDatasetDepublication() {

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
        DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

    try {
      validator.validate(dpsTask);
      Assert.fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.METIS_DATASET_ID));
    }
  }

  @Test
  public void shouldFailBecauseOfMissingRequiredMetisDepublicationReasonParameterForDatasetDepublication() {

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");

    DpsTaskValidator validator =
        DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

    try {
      validator.validate(dpsTask);
      Assert.fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.DEPUBLICATION_REASON));
    }
  }

  @Test
  public void shouldFailBecauseOfMissingRequiredMetisDatasetIdParameterForRecordsDepublication() {

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
        DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    try {
      validator.validate(dpsTask);
      Assert.fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.METIS_DATASET_ID));
    }
  }

  @Test
  public void shouldFailBecauseOfMissingRequiredRecordIdsParameterForRecordsDepublication() {

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
        DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    try {
      validator.validate(dpsTask);
      Assert.fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH));
    }
  }

  @Test
  public void shouldFailBecauseOfMissingRequiredDepublicationReasonForRecordsDepublication() {

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, "records");

    DpsTaskValidator validator =
        DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    try {
      validator.validate(dpsTask);
      Assert.fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(PluginParameterKeys.DEPUBLICATION_REASON));
    }

  }

  @Test
  public void shouldSuccessfullyValidateTheTaskForDatasetDepublication() throws DpsTaskValidationException {

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
        DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_DATASET);

    validator.validate(dpsTask);
  }

  @Test
  public void shouldSuccessfullyValidateTheTaskForRecordsDepublication() throws DpsTaskValidationException {

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, "metisDS");
    dpsTask.addParameter(PluginParameterKeys.DEPUBLICATION_REASON, "reason");

    DpsTaskValidator validator =
        DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.DEPUBLICATION_TASK_FOR_RECORDS);

    validator.validate(dpsTask);
  }

}
