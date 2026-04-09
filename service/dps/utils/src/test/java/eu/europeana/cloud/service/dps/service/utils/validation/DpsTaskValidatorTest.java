package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.DatasetRevisionInfo;
import eu.europeana.cloud.service.dps.DatasetRevisionInfo.DatasetRevisionInfoBuilder;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.FilesUrls;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.FullyDefinedMCSInputValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DpsTaskValidatorTest {

  private DpsTask dpsTask;
  private DpsTask icTopologyTask;
  private DpsTask dpsTaskWithIncorrectRevision_1;
  private DpsTask dpsTaskWithIncorrectRevision_2;
  private DpsTask dpsTaskWithIncorrectRevision_3;
  private DpsTask dpsTaskWithIncorrectRevision_4;
  private DpsTask dpsTaskWithIncorrectRevision_5;
  private DpsTask dpsTaskWithIncorrectRevision_6;
  private DpsTask dpsTaskWithIncorrectRevision_7;
  private DpsTask dpsTaskWithIncorrectRevision_8;
  private DpsTask dpsTaskWithIncorrectRevision_9;
  private DpsTask dpsTaskWithIncorrectRevision_10;
  private DpsTask dpsTaskWithNullOutputRevision;
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

  private static final Revision correctRevision = new Revision("sampleRevisionName", "sampleRevisionProvider");


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
    dpsTaskWithIncorrectRevision_1 = new DpsTask(TASK_NAME);
    Revision r1 = new Revision();
    dpsTaskWithIncorrectRevision_1.setOutput(prepareCompleteDatasetRevisionInfo().revision(r1).build());
    //
    dpsTaskWithIncorrectRevision_2 = new DpsTask(TASK_NAME);
    Revision revisionWithoutProviderId = new Revision();
    revisionWithoutProviderId.setRevisionName("sampleRevisionName");
    dpsTaskWithIncorrectRevision_2.setOutput(prepareCompleteDatasetRevisionInfo().revision(revisionWithoutProviderId).build());
    //
    dpsTaskWithIncorrectRevision_3 = new DpsTask(TASK_NAME);
    Revision revisionWithoutName = new Revision();
    revisionWithoutName.setRevisionProviderId("sampleRevisionProvider");
    dpsTaskWithIncorrectRevision_3.setOutput(prepareCompleteDatasetRevisionInfo().revision(revisionWithoutName).build());
    //
    dpsTaskWithIncorrectRevision_4 = new DpsTask(TASK_NAME);
    Revision revisionWithEmptyName = new Revision();
    revisionWithEmptyName.setRevisionProviderId("sampleRevisionProvider");
    revisionWithEmptyName.setRevisionName("");
    dpsTaskWithIncorrectRevision_4.setOutput(prepareCompleteDatasetRevisionInfo().revision(revisionWithEmptyName).build());
    //
    dpsTaskWithIncorrectRevision_5 = new DpsTask(TASK_NAME);
    Revision revisionWithEmptyProviderId = new Revision();
    revisionWithEmptyProviderId.setRevisionProviderId("");
    revisionWithEmptyProviderId.setRevisionName("sampleRevisionName");
    dpsTaskWithIncorrectRevision_5.setOutput(prepareCompleteDatasetRevisionInfo().revision(revisionWithEmptyProviderId).build());
    //
    dpsTaskWithIncorrectRevision_6 = new DpsTask(TASK_NAME);
    Revision revisionWithEmptyProviderIdAndName = new Revision();
    revisionWithEmptyProviderIdAndName.setRevisionProviderId("");
    revisionWithEmptyProviderIdAndName.setRevisionName("");
    dpsTaskWithIncorrectRevision_6.setOutput(prepareCompleteDatasetRevisionInfo().revision(revisionWithEmptyProviderIdAndName).build());
    //
    dpsTaskWithIncorrectRevision_7 = new DpsTask(TASK_NAME);
    Revision revisionWithEmptyProviderId_1 = new Revision();
    revisionWithEmptyProviderId_1.setRevisionProviderId("  ");
    revisionWithEmptyProviderId_1.setRevisionName("sampleRevisionName");
    dpsTaskWithIncorrectRevision_7.setOutput(prepareCompleteDatasetRevisionInfo().revision(revisionWithEmptyProviderId_1).build());
    //
    dpsTaskWithIncorrectRevision_8 = new DpsTask(TASK_NAME);
    Revision revisionWithEmptyName_1 = new Revision();
    revisionWithEmptyName_1.setRevisionProviderId("sampleProviderId");
    revisionWithEmptyName_1.setRevisionName(" ");
    dpsTaskWithIncorrectRevision_8.setOutput(prepareCompleteDatasetRevisionInfo().revision(revisionWithEmptyName_1).build());
    //
    dpsTaskWithIncorrectRevision_9 = new DpsTask(TASK_NAME);
    Revision revisionWithEmptyProviderIdAndName_1 = new Revision();
    revisionWithEmptyProviderIdAndName_1.setRevisionProviderId(" ");
    revisionWithEmptyProviderIdAndName_1.setRevisionName("  ");
    dpsTaskWithIncorrectRevision_9.setOutput(prepareCompleteDatasetRevisionInfo().revision(revisionWithEmptyProviderIdAndName_1).build());
    //
      dpsTaskWithIncorrectRevision_10 = new DpsTask(TASK_NAME);
      dpsTaskWithIncorrectRevision_10.setInput(prepareCompleteDatasetRevisionInfo()
                                                                  .revision(
                                                                      Revision.builder()
                                                                              .revisionProviderId("sampleProvider")
                                                                              .revisionName("sampleRevisionName").build())
                                                                  .build());


    dpsTaskWithNullOutputRevision = new DpsTask(TASK_NAME);

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

  public static DatasetRevisionInfoBuilder prepareCompleteDatasetRevisionInfo() {
    return DatasetRevisionInfo.builder().providerId("providerId").datasetId("datasetId").
                              representationName("representationName")
                              .revision(new Revision("sampleRevisionName", "sampleRevisionProvider", new Date()));
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

    ////
    //Output Revision
    ////
    @Test
    void validatorShouldValidateThatOutputRevisionIsNotCorrect_1() {
        assertThrows(DpsTaskValidationException.class, () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithIncorrectRevision_1));
    }

    @Test
    void validatorShouldValidateThatOutputRevisionIsNotCorrect_2() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithIncorrectRevision_2));
    }

    @Test
    void validatorShouldValidateThatOutputRevisionIsNotCorrect_3() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithIncorrectRevision_3));
    }

    @Test
    void validatorShouldValidateThatOutputRevisionIsNotCorrect_4() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithIncorrectRevision_4));
    }

    @Test
    void validatorShouldValidateThatOutputRevisionIsNotCorrect_5() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithIncorrectRevision_5));
    }

  @Test
  void validatorShouldValidateThatOutputRevisionIsNotCorrect_6() {
    assertThrows(DpsTaskValidationException.class,
            () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithIncorrectRevision_6));
  }

    @Test
    void validatorShouldValidateThatOutputRevisionIsNotCorrect_7() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithIncorrectRevision_7));
    }

    @Test
    void validatorShouldValidateThatOutputRevisionIsNotCorrect_8() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithIncorrectRevision_8));
    }

    @Test
    void validatorShouldValidateThatOutputRevisionIsNotCorrect_9() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithIncorrectRevision_9));
    }

    @Test
    void validatorShouldValidateThatInputRevisionIsNotCorrect_10() {
        try {
            new DpsTaskValidator().withCustomValidator(new FullyDefinedMCSInputValidator())
                    .validate(dpsTaskWithIncorrectRevision_10);
            fail("Should fail on FullyDefinedOutputRevisionValidator");
        } catch (DpsTaskValidationException e) {
            assertTrue(e.getMessage().contains(FullyDefinedMCSInputValidator.ERROR_MESSAGE));
        }
    }

    @Test
    void validatorShouldValidateTheRequiredNullOutputRevisionAsNotCorrect() {
        assertThrows(DpsTaskValidationException.class,
                () -> new DpsTaskValidator().withDefinedMCSOutput().validate(dpsTaskWithNullOutputRevision));
    }

    @Test
    void validatorShouldValidateDpsTaskWithCorrectOutputRevision() throws DpsTaskValidationException {
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
