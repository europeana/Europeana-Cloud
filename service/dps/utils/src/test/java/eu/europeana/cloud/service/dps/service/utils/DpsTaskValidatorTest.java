package eu.europeana.cloud.service.dps.service.utils;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DpsTaskValidatorTest {

    private static final long TASK_ID = 121212;
    private DpsTask dpsTask;

    private static final String TASK_NAME = "taksName";
    private static final String EXISTING_PARAMETER_NAME = "param_1";
    private static final String EXISTING_PARAMETER_VALUE = "param_1_value";
    private static final String EMPTY_PARAMETER_NAME = "empty_param";

    private static final String EXISTING_DATA_ENTRY_NAME = "dataEntryName";
    private static final List<String> EXISTING_DATA_ENTRY_VALUE = Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt");

    @Before
    public void init() {
        dpsTask = new DpsTask();
        dpsTask.setTaskName(TASK_NAME);
        dpsTask.addParameter(EXISTING_PARAMETER_NAME, EXISTING_PARAMETER_VALUE);
        dpsTask.addParameter(EMPTY_PARAMETER_NAME, "");
        dpsTask.addDataEntry(EXISTING_DATA_ENTRY_NAME, EXISTING_DATA_ENTRY_VALUE);
    }

    ////
    // Task id
    ////
    @Test
    public void anyIdShouldBeValidated() throws DpsTaskValidationException {
        new DpsTaskValidator().withAnyId().validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void shouldThrowExceptionForWrongTaskId() throws DpsTaskValidationException {
        new DpsTaskValidator().withId(-1).validate(dpsTask);
    }

    ////
    //Task name
    ////
    @Test
    public void validatorShouldValidateThatTaskHasSelectedName() throws DpsTaskValidationException {
        new DpsTaskValidator().withName(TASK_NAME).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatTaskHasWrongName() throws DpsTaskValidationException {
        new DpsTaskValidator().withName("someWrongName").validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatTaskHasAnyName() throws DpsTaskValidationException {
        new DpsTaskValidator().withAnyName().validate(dpsTask);
    }

    ////
    //Parameters
    ////
    @Test
    public void validatorShouldValidateThatThereIsParameterWithSelectedNameAndAnyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter(EXISTING_PARAMETER_NAME).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsNoParameterWithSelectedName() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter("p2").validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsSelectedParameterWithCorrectValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter(EXISTING_PARAMETER_NAME, EXISTING_PARAMETER_VALUE).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsSelectedParameterWithWrongValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter("p1", "p3").validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsSelectedParameterWithEmptyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withEmptyParameter(EMPTY_PARAMETER_NAME).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsNoSelectedParameterWithEmptyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withEmptyParameter("nonEmptyParameter").validate(dpsTask);
    }
    ////
    //inputData
    ////

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsNoInputDataWithSelectedName() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry("notExistingDataEntry").validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndAnyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME).validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndCorrectValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt")).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndWrongValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME, Arrays.asList("someWrongValue")).validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndCorrectContentType() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME, InputDataValueType.LINK_TO_FILE).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndIncorrectContentType() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME, InputDataValueType.LINK_TO_DATASET).validate(dpsTask);
    }
}
