package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.FilesUrls;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DpsTaskValidatorTest {

  private DpsTask dpsTask;
  private DpsTask icTopologyTask;
  private DpsTask dpsTaskWithValidMaximumParallelization;
  private DpsTask dpsTaskWithNotNumberMaximumParallelization;
  private DpsTask dpsTaskWithTooBigPossibleMaximumParallelization;
  private DpsTask dpsTaskWithZeroMaximumParallelization;
  private DpsTask dpsTaskWithNegativeMaximumParallelization;
  private DpsTask dpsTaskWithMinimalValidMaximumParallelization;

  private static final String TASK_NAME = "taskName";
  private static final String EXISTING_PARAMETER_NAME = "param_1";
  private static final String EXISTING_PARAMETER_VALUE = "param_1_value";
  private static final String EMPTY_PARAMETER_NAME = "empty_param";

  private static final List<String> EXISTING_DATA_ENTRY_VALUE =
      Collections.singletonList(
          "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt");

    @BeforeEach
    void init() {
      dpsTask = new DpsTask();
      dpsTask.setTaskName(TASK_NAME);
      dpsTask.addParameter(EXISTING_PARAMETER_NAME, EXISTING_PARAMETER_VALUE);
      dpsTask.addParameter(EMPTY_PARAMETER_NAME, "");
      dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
      dpsTask.setInput(prepareCompleteDatasetRevisionInfo().build());
      dpsTask.setOutput(prepareCompleteDatasetRevisionInfo().build());
      //
      icTopologyTask = new DpsTask();
      icTopologyTask.setInput(new FilesUrls(
            "https://iks-kbase.synat.pcss.pl:9090/mcs/records/JP46FLZLVI2UYV4JNHTPPAB4DGPESPY4SY4N5IUQK4SFWMQ3NUQQ/representations/tiff/versions/74c56880-7733-11e5-b38f-525400ea6731/files/f59753a5-6d75-4d48-9f4d-4690b671240c",
            "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
        ));
    icTopologyTask.addParameter("OUTPUT_MIME_TYPE", "image/jp2");
    icTopologyTask.addParameter("SAMPLE_PARAMETER", "sampleParameterValue");
    //


    dpsTaskWithMinimalValidMaximumParallelization = new DpsTask(TASK_NAME);
    dpsTaskWithMinimalValidMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "1");

    dpsTaskWithValidMaximumParallelization = new DpsTask(TASK_NAME);
    dpsTaskWithValidMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "10");

    dpsTaskWithNegativeMaximumParallelization = new DpsTask(TASK_NAME);
    dpsTaskWithNegativeMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "-2");

    dpsTaskWithZeroMaximumParallelization = new DpsTask(TASK_NAME);
    dpsTaskWithZeroMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "0");

    dpsTaskWithTooBigPossibleMaximumParallelization = new DpsTask(TASK_NAME);
    dpsTaskWithTooBigPossibleMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION,
        String.valueOf(1L + Integer.MAX_VALUE));

    dpsTaskWithNotNumberMaximumParallelization = new DpsTask(TASK_NAME);
    dpsTaskWithNotNumberMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "a");

  }

  public static BatchInfo.BatchInfoBuilder prepareCompleteDatasetRevisionInfo() {
    return BatchInfo.builder().providerId("providerId").batchId("datasetId").
                              representationName("representationName");
  }

  @Test
    void shouldValidateThatTaskIsCorrectWhenConstraintsListIsEmpty() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTask);
    }

    ////
    // Task id
    ////
    @Test
    void anyIdShouldBeValidated() throws DpsTaskValidationException {
        new DpsTaskValidator().withAnyId().validate(dpsTask);
    }

    @Test
    void shouldThrowExceptionForWrongTaskId() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withId(-1).validate(dpsTask));
    }

    ////
    //Task name
    ////
    @Test
    void validatorShouldValidateThatTaskHasSelectedName() throws DpsTaskValidationException {
        new DpsTaskValidator().withName(TASK_NAME).validate(dpsTask);
    }

    @Test
    void validatorShouldValidateThatTaskHasWrongName() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withName("someWrongName").validate(dpsTask));
    }

    @Test
    void validatorShouldValidateThatTaskHasAnyName() throws DpsTaskValidationException {
        new DpsTaskValidator().withAnyName().validate(dpsTask);
    }

  ////
  //Parameters
  ////
  @Test
  void validatorShouldValidateThatThereIsParameterWithSelectedNameAndAnyValue() throws DpsTaskValidationException {
    new DpsTaskValidator().withParameter(EXISTING_PARAMETER_NAME).validate(dpsTask);
  }

    @Test
    void validatorShouldValidateThatThereIsNoParameterWithSelectedName() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withParameter("p2").validate(dpsTask));
    }

    @Test
    void validatorShouldValidateThatThereIsSelectedParameterWithCorrectValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter(EXISTING_PARAMETER_NAME, EXISTING_PARAMETER_VALUE).validate(dpsTask);
    }

    @Test
    void validatorShouldValidateThatThereIsSelectedParameterWithWrongValue() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withParameter("p1", "p3").validate(dpsTask));
    }

    @Test
    void validatorShouldValidateThatThereIsSelectedParameterWithEmptyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withEmptyParameter(EMPTY_PARAMETER_NAME).validate(dpsTask);
    }

    @Test
    void validatorShouldValidateThatThereIsSelectedParameterWithOneOfAllowedValues() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE,
                TargetIndexingDatabase.getTargetIndexingDatabaseValues()).validate(dpsTask);
    }

    @Test
    void validatorShouldValidateThatThereIsNoSelectedParameterWithEmptyValue() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withEmptyParameter("nonEmptyParameter").validate(dpsTask));
    }

    @Test
    void shouldDiscardTaskWithMissingDataSetName() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withParameter(PluginParameterKeys.METIS_DATASET_ID).validate(dpsTask));
    }

    @Test
    void shouldValidateTaskCorrectlyWithDataSetName() throws DpsTaskValidationException {
        dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "sample");
        new DpsTaskValidator().withParameter(PluginParameterKeys.METIS_DATASET_ID).validate(dpsTask);
    }


    ////
    //inputData
    ////

    @Test
    void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndAnyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withDefinedMCSInput().validate(dpsTask);
    }

    @Test
    void shouldValidateTaskForICTopology() throws DpsTaskValidationException {
        new DpsTaskValidator()
                .withDefinedFilesUrlsInput()
                .withParameter("OUTPUT_MIME_TYPE")
                .withParameter("SAMPLE_PARAMETER")
                .validate(icTopologyTask);
    }

    @ParameterizedTest
    @CsvSource({"domain.broken/path",
            "badproto://domainlondon/paht/some.xml", "domainlon.com/path/some.xml"})
    void shouldTrowExceptionValidateTaskForOaiPmhTopology(String url) {
        assertThrows(DpsTaskValidationException.class, () ->
                commonOaiPmhValidation(url));
    }

    @ParameterizedTest
    @CsvSource({"https://domain.com/data-providers/xxx1/data-sets/yyy1",
            "https://domain.com/data-providers/xxx2/data-sets/yyy2", "https://domain.com/data-providers/xxx3/data-sets/yyy3"})
    void shouldValidateTaskForOaiPmhTopology(String url) throws DpsTaskValidationException {
        commonOaiPmhValidation(url);
    }

    private void commonOaiPmhValidation(String url) throws DpsTaskValidationException {
        final DpsTask oaiPmhTask = new DpsTask("OaiPmhTopology");
        final OAIPMHHarvestingDetails inputData = OAIPMHHarvestingDetails.builder()
            .repositoryUrl(url).build();
        oaiPmhTask.setInput(inputData);

    new DpsTaskValidator()
        .withDefinedOAIInput()
        .validate(oaiPmhTask);
    }

    @Test
    void validatorShouldValidateDpsTaskWithCorrectOutputBatch() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTask);
    }

    @Test
    void validatorShouldValidateDpsTaskWithValidMaximumParallelization() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithValidMaximumParallelization);
    }

    @Test
    void validatorShouldValidateDpsTaskWithMinimalValidMaximumParallelization() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithMinimalValidMaximumParallelization);
    }

    @Test
    void validatorShouldValidateThatNegativeMaximumParallelizationIsNotCorrect() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().validate(dpsTaskWithNegativeMaximumParallelization));
    }

    @Test
    void validatorShouldValidateThatZeroMaximumParallelizationIsNotCorrect() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().validate(dpsTaskWithZeroMaximumParallelization));
    }

    @Test
    void validatorShouldValidateThatTooBigPossibleMaximumParallelizationIsNotCorrect() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().validate(dpsTaskWithTooBigPossibleMaximumParallelization));
    }

    @Test
    void validatorShouldValidateThatNotNumberMaximumParallelizationIsNotCorrect() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().validate(dpsTaskWithNotNumberMaximumParallelization));
    }

}
