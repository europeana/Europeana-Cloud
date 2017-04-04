package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.dps.DpsTask;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

public class DpsTaskValidator {

    private List<DpsTaskConstraint> dpsTaskConstraints = new ArrayList<>();
    private String validatorName;

    public DpsTaskValidator() {
        this("Default validator");
    }

    public DpsTaskValidator(String validatorName) {
        this.validatorName = validatorName;
        addDefaultConstraints();
    }

    public DpsTaskValidator withParameter(String parameterName) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.PARAMETER, parameterName);

        dpsTaskConstraints.add(constraint);
        return this;

    }

    public String getValidatorName() {
        return validatorName;
    }

    /**
     * Will check if dps task contains parameter with selected name and selected value
     *
     * @param parameterName
     * @param parameterValue
     * @return
     */
    public DpsTaskValidator withParameter(String parameterName, String parameterValue) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.PARAMETER, parameterName, parameterValue);

        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains parameter with selected name (value of this parameter will not be validated)
     *
     * @param parameterName
     * @return
     */
    public DpsTaskValidator withEmptyParameter(String parameterName) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.PARAMETER, parameterName, "");

        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains input data with selected name (value of this input data will not be validated)
     *
     * @param inputDataName
     * @return
     */
    public DpsTaskValidator withDataEntry(String inputDataName) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.INPUT_DATA, inputDataName);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains input data with selected name and selected value
     *
     * @param entryName
     * @param entryValue
     * @return
     */
    public DpsTaskValidator withDataEntry(String entryName, Object entryValue) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.INPUT_DATA, entryName, entryValue);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains input data with selected name and selected content type
     *
     * @param entryName
     * @param contentType content type of input data entry (can be file url, dataset url, ...)
     * @return
     */
    public DpsTaskValidator withDataEntry(String entryName, InputDataValueType contentType) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.INPUT_DATA, entryName, contentType);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains selected name
     *
     * @param taskName
     * @return
     */
    public DpsTaskValidator withName(String taskName) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.NAME, null, taskName);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains any name
     *
     * @return
     */
    public DpsTaskValidator withAnyName() {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.NAME);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains selected task id
     *
     * @param taskId
     * @return
     */
    public DpsTaskValidator withId(long taskId) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.ID, null, taskId + "");
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains any task id
     *
     * @return
     */
    public DpsTaskValidator withAnyId() {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.ID);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    public void validate(DpsTask task) throws DpsTaskValidationException {
        int constraintsNumber = dpsTaskConstraints.size();

        for (DpsTaskConstraint re : dpsTaskConstraints) {
            DpsTaskFieldType fieldType = re.getFieldType();
            if (fieldType.equals(DpsTaskFieldType.NAME)) {
                validateName(task, re);
            } else if (fieldType.equals(DpsTaskFieldType.PARAMETER)) {
                validateParameter(task, re);
            } else if (fieldType.equals(DpsTaskFieldType.INPUT_DATA)) {
                validateInputData(task, re);
            } else if (fieldType.equals(DpsTaskFieldType.ID)) {
                validateId(task, re);
            } else if (fieldType.equals(DpsTaskFieldType.OUTPUT_REVISION)) {
                validateOutputRevision(task);
            }
        }
    }

    private void addDefaultConstraints() {
        DpsTaskConstraint outputRevisionConstraint = new DpsTaskConstraint(DpsTaskFieldType.OUTPUT_REVISION);
        dpsTaskConstraints.add(outputRevisionConstraint);
    }

    private void validateName(DpsTask task, DpsTaskConstraint constraint) throws DpsTaskValidationException {
        String taskName = task.getTaskName();
        if (constraint.getExpectedValue() == null && taskName != null) {  //any name
            return;
        }
        if ("".equals(constraint.getExpectedValue()) && "".equals(taskName)) {//empty name
            return;
        }
        if (constraint.getExpectedValue().equals(taskName)) {//exact name
            return;
        }

        throw new DpsTaskValidationException("Task name is not valid.");
    }

    private void validateParameter(DpsTask task, DpsTaskConstraint constraint) throws DpsTaskValidationException {
        String expectedParameter = task.getParameter(constraint.getExpectedName());
        if (expectedParameter == null) {
            throw new DpsTaskValidationException("Expected parameter does not exist in dpsTask. Parameter name: " + constraint.getExpectedName());
        }
        if (constraint.getExpectedValue() == null && expectedParameter != null) {  //any name
            return;
        }
        if ("".equals(constraint.getExpectedValue()) && "".equals(expectedParameter)) {  //empty value
            return;
        }
        if (expectedParameter.equals(constraint.getExpectedValue())) {  //exact value
            return;
        }
        throw new DpsTaskValidationException("Parameter does not meet constraints. Parameter name: " + constraint.getExpectedName());
    }

    private void validateInputData(DpsTask task, DpsTaskConstraint constraint) throws DpsTaskValidationException {
        List<String> expectedInputData = task.getDataEntry(constraint.getExpectedName());

        if (expectedInputData == null) {
            throw new DpsTaskValidationException("Expected parameter does not exist in dpsTask. Parameter name: " + constraint.getExpectedName());
        }
        if (constraint.getExpectedValueType() != null) {
            validateInputDataContent(expectedInputData, constraint);
        }
        if (constraint.getExpectedValue() == null && expectedInputData != null) {   //any value
            return;
        }
        if ("".equals(constraint.getExpectedValue()) && expectedInputData.size() == 0) {    //empty value
            return;
        }
        if (expectedInputData.equals(constraint.getExpectedValue())) {  //exact value
            return;
        }
        throw new DpsTaskValidationException("Input data is not valid.");
    }

    private void validateInputDataContent(List<String> expectedInputData, DpsTaskConstraint constraint) throws DpsTaskValidationException {
        for (String expectedInputDataValue : expectedInputData) {
            if (constraint.getExpectedValueType().equals(InputDataValueType.LINK_TO_FILE)) {
                try {
                    UrlParser parser = new UrlParser(expectedInputDataValue);
                    if (parser.isUrlToRepresentationVersionFile()) {
                        continue;
                    }
                    throw new DpsTaskValidationException("Wrong input data: " + expectedInputDataValue);
                } catch (MalformedURLException e) {
                    throw new DpsTaskValidationException("Wrong input data: " + expectedInputDataValue);
                }
            } else if (constraint.getExpectedValueType().equals(InputDataValueType.LINK_TO_DATASET)) {
                try {
                    UrlParser parser = new UrlParser(expectedInputDataValue);
                    if (parser.isUrlToDataset()) {
                        continue;
                    }
                    throw new DpsTaskValidationException("Wrong input data: " + expectedInputDataValue);
                } catch (MalformedURLException e) {
                    throw new DpsTaskValidationException("Wrong input data: " + expectedInputDataValue);
                }
            }
        }
    }

    private void validateId(DpsTask task, DpsTaskConstraint constraint) throws DpsTaskValidationException {
        long taskId = task.getTaskId();
        if (constraint.getExpectedValue() == null) {  //any id
            return;
        }
        if (constraint.getExpectedValue().equals(taskId + "")) {//exacted id
            return;
        }
        throw new DpsTaskValidationException("Task id is not valid.");
    }

    private void validateOutputRevision(DpsTask task) throws DpsTaskValidationException {
        Revision outputRevision = task.getOutputRevision();
        if (outputRevision != null) {
            if (outputRevision.getRevisionName() == null || outputRevision.getRevisionProviderId() == null) {
                throw new DpsTaskValidationException("Revision name and revision provider has to be not null");
            }
        }
    }
}

/**
 * Holds the definition of single constraint that should be fullfiled by dpsTask
 */
class DpsTaskConstraint {
    private DpsTaskFieldType fieldType;
    private Object expectedValue;
    private InputDataValueType expectedValueType;
    private String expectedName;

    public DpsTaskConstraint(DpsTaskFieldType fieldType, String expectedName, Object expectedValue) {
        this.fieldType = fieldType;
        this.expectedName = expectedName;
        this.expectedValue = expectedValue;
    }

    public DpsTaskConstraint(DpsTaskFieldType fieldType, String expectedName, InputDataValueType expectedValueType) {
        this.fieldType = fieldType;
        this.expectedName = expectedName;
        this.expectedValueType = expectedValueType;
    }

    public DpsTaskConstraint(DpsTaskFieldType fieldType, String expectedName) {
        this.fieldType = fieldType;
        this.expectedName = expectedName;
    }

    public DpsTaskConstraint(DpsTaskFieldType fieldType) {
        this.fieldType = fieldType;
    }

    public Object getExpectedValue() {
        return expectedValue;
    }

    public InputDataValueType getExpectedValueType() {
        return expectedValueType;
    }

    public DpsTaskFieldType getFieldType() {
        return fieldType;
    }

    public String getExpectedName() {
        return expectedName;
    }
}

enum DpsTaskFieldType {
    PARAMETER,
    INPUT_DATA,
    ID,
    NAME,
    OUTPUT_REVISION;
}