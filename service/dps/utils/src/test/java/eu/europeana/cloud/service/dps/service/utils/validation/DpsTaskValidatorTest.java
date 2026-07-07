package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class DpsTaskValidatorTest {

  private static final String PROVIDER = "provider1" ;
  private static final String BATCH = "batch1";
  public static final String OUTPUT_PROVIDER = "providerId";
  private CreateDpsTaskRequest dpsTask;
  private CreateDpsTaskRequest icTopologyTask;
  private CreateDpsTaskRequest dpsTaskWithValidMaximumParallelization;
  private CreateDpsTaskRequest dpsTaskWithNotNumberMaximumParallelization;
  private CreateDpsTaskRequest dpsTaskWithTooBigPossibleMaximumParallelization;
  private CreateDpsTaskRequest dpsTaskWithZeroMaximumParallelization;
  private CreateDpsTaskRequest dpsTaskWithNegativeMaximumParallelization;
  private CreateDpsTaskRequest dpsTaskWithMinimalValidMaximumParallelization;

  private static final String EXISTING_PARAMETER_NAME = "param_1";
  private static final String EXISTING_PARAMETER_VALUE = "param_1_value";
  private static final String EMPTY_PARAMETER_NAME = "empty_param";

    @BeforeEach
    void init() {
      dpsTask = new CreateDpsTaskRequest();
      dpsTask.addParameter(EXISTING_PARAMETER_NAME, EXISTING_PARAMETER_VALUE);
      dpsTask.addParameter(EMPTY_PARAMETER_NAME, "");
      dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
      dpsTask.setInput(prepareCompleteDatasetRevisionInfo().build());
      dpsTask.setOutputProvider(OUTPUT_PROVIDER);
      //
      icTopologyTask = new CreateDpsTaskRequest();
      icTopologyTask.setInput(BatchInfo.builder().providerId(PROVIDER).batchId(BATCH).build());
    icTopologyTask.addParameter("OUTPUT_MIME_TYPE", "image/jp2");
    icTopologyTask.addParameter("SAMPLE_PARAMETER", "sampleParameterValue");
    //


    dpsTaskWithMinimalValidMaximumParallelization = new CreateDpsTaskRequest();
    dpsTaskWithMinimalValidMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "1");

    dpsTaskWithValidMaximumParallelization = new CreateDpsTaskRequest();
    dpsTaskWithValidMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "10");

    dpsTaskWithNegativeMaximumParallelization = new CreateDpsTaskRequest();
    dpsTaskWithNegativeMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "-2");

    dpsTaskWithZeroMaximumParallelization = new CreateDpsTaskRequest();
    dpsTaskWithZeroMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "0");

    dpsTaskWithTooBigPossibleMaximumParallelization = new CreateDpsTaskRequest();
    dpsTaskWithTooBigPossibleMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION,
        String.valueOf(1L + Integer.MAX_VALUE));

    dpsTaskWithNotNumberMaximumParallelization = new CreateDpsTaskRequest();
    dpsTaskWithNotNumberMaximumParallelization.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, "a");

  }

  public static BatchInfo.BatchInfoBuilder prepareCompleteDatasetRevisionInfo() {
    return BatchInfo.builder().providerId("providerId").batchId("datasetId");
  }

  @Test
    void shouldValidateThatTaskIsCorrectWhenConstraintsListIsEmpty() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTask);
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
                .withDefinedMCSInput()
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
        final CreateDpsTaskRequest oaiPmhTask = new CreateDpsTaskRequest();
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
