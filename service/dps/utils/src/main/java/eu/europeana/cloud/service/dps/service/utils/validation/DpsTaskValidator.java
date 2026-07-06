package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.CustomValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.FullyDefinedMCSInputValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.HttpInputValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.NoOutputValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.OAIInputValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.ProperlyDefinedMCSOutputValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.MaximumParallelizationValidator;

import java.util.ArrayList;
import java.util.List;

public final class DpsTaskValidator {

  private final List<DpsTaskConstraint> dpsTaskConstraints = new ArrayList<>();
  private final List<CustomValidator> customValidators = new ArrayList<>();
  private final String validatorName;

  public DpsTaskValidator() {
    this("Default validator");
  }

  public DpsTaskValidator(String validatorName) {
    this.validatorName = validatorName;
    withCustomValidator(new MaximumParallelizationValidator());
  }

  public DpsTaskValidator withParameter(String parameterName) {
    DpsTaskConstraint constraint = DpsTaskConstraint.newDpsTaskConstraint()
                                                    .fieldType(DpsTaskFieldType.PARAMETER)
                                                    .expectedName(parameterName)
                                                    .build();
    dpsTaskConstraints.add(constraint);
    return this;

  }

  /**
   * Will check if dps task contains parameter with selected name and selected value
   *
   * @param parameterName parameter with this name will be validated
   * @param parameterValue parameter with the provided name will be validated against this value
   * @return currently constructed validator
   */
  public DpsTaskValidator withParameter(String parameterName, String parameterValue) {
    DpsTaskConstraint constraint = DpsTaskConstraint.newDpsTaskConstraint()
                                                    .fieldType(DpsTaskFieldType.PARAMETER)
                                                    .expectedName(parameterName)
                                                    .expectedValue(parameterValue)
                                                    .build();
    dpsTaskConstraints.add(constraint);
    return this;
  }


  /**
   * Will check if dps task contains parameter with selected name and any of the allowed values
   *
   * @param paramName parameter name
   * @param allowedValues list of allowed values
   * @return currently constructed validator
   */
  public DpsTaskValidator withParameter(String paramName, List allowedValues) {
    DpsTaskConstraint constraint = DpsTaskConstraint.newDpsTaskConstraint()
                                                    .fieldType(DpsTaskFieldType.PARAMETER)
                                                    .expectedName(paramName)
                                                    .expectedValue(allowedValues)
                                                    .build();

    dpsTaskConstraints.add(constraint);
    return this;
  }

  /**
   * Will check if dps task contains parameter with selected name and no value
   *
   * @param parameterName parameter with this name will be validated
   * @return currently constructed validator
   */
  public DpsTaskValidator withEmptyParameter(String parameterName) {
    DpsTaskConstraint constraint = DpsTaskConstraint.newDpsTaskConstraint()
                                                    .fieldType(DpsTaskFieldType.PARAMETER)
                                                    .expectedName(parameterName)
                                                    .expectedValue("")
                                                    .build();

    dpsTaskConstraints.add(constraint);
    return this;
  }

  public DpsTaskValidator withDefinedMCSOutput() {
    return withCustomValidator(new ProperlyDefinedMCSOutputValidator());
  }

  public DpsTaskValidator withDefinedMCSInput() {
    return withCustomValidator(new FullyDefinedMCSInputValidator());
  }

  public DpsTaskValidator withDefinedOAIInput() {
    return withCustomValidator(new OAIInputValidator());
  }

  public DpsTaskValidator withDefinedHttpInput() {
    return withCustomValidator(new HttpInputValidator());
  }

  public DpsTaskValidator withNoOutput() {
    return withCustomValidator(new NoOutputValidator());
  }

  public DpsTaskValidator withCustomValidator(CustomValidator validator) {
    customValidators.add(validator);
    return this;
  }

  public void validate(DpsTask task) throws DpsTaskValidationException {
    for (DpsTaskConstraint re : dpsTaskConstraints) {
      DpsTaskFieldType fieldType = re.getFieldType();
      if (fieldType == DpsTaskFieldType.PARAMETER) {
        validateParameter(task, re);
      }
    }
    for (CustomValidator customValidator : customValidators) {
      if (!customValidator.test(task)) {
        throw new DpsTaskValidationException(
            "Dps Task doesn't meet the requirements of the custom validator. " + customValidator.errorMessage());
      }
    }
  }

  private void validateParameter(DpsTask task, DpsTaskConstraint constraint) throws DpsTaskValidationException {
    String parameterValue = task.getParameter(constraint.getExpectedName());
    if (parameterValue == null) {
      throw new DpsTaskValidationException(
          "Expected parameter does not exist in dpsTask. Parameter name: " + constraint.getExpectedName());
    }
    Object expectedValue = constraint.getExpectedValue();
    if (expectedValue == null) {  //any name
      return;
    }
    if (expectedValue instanceof List) {
      List<String> ls = (List) expectedValue;
      if (ls.contains(parameterValue)) {
        return;
      }
    } else {
      if ("".equals(expectedValue) && "".equals(parameterValue)) {  //empty value
        return;
      }
      if (parameterValue.equals(expectedValue)) {  //exact value
        return;
      }
    }
    throw new DpsTaskValidationException("Parameter does not meet constraints. Parameter name: " + constraint.getExpectedName());
  }
}
/**
 * Holds the definition of single constraint that should be fulfilled by dpsTask
 */
class DpsTaskConstraint {

  private final DpsTaskFieldType fieldType;
  private final Object expectedValue;
  private final InputDataValueType expectedValueType;
  private final String expectedName;

  private DpsTaskConstraint(Builder builder) {
    this.fieldType = builder.fieldType;
    this.expectedValue = builder.expectedValue;
    this.expectedValueType = builder.expectedValueType;
    this.expectedName = builder.expectedName;
  }

  public static Builder newDpsTaskConstraint() {
    return new Builder();
  }

  public Object getExpectedValue() {
    return expectedValue;
  }

  public DpsTaskFieldType getFieldType() {
    return fieldType;
  }

  public String getExpectedName() {
    return expectedName;
  }


  public static final class Builder {

    private DpsTaskFieldType fieldType;
    private Object expectedValue;
    private InputDataValueType expectedValueType;
    private String expectedName;

    private Builder() {
    }

    public DpsTaskConstraint build() {
      return new DpsTaskConstraint(this);
    }

    public Builder fieldType(DpsTaskFieldType fieldType) {
      this.fieldType = fieldType;
      return this;
    }

    public Builder expectedValue(Object expectedValue) {
      this.expectedValue = expectedValue;
      return this;
    }

    public Builder expectedName(String expectedName) {
      this.expectedName = expectedName;
      return this;
    }
  }
}

enum DpsTaskFieldType {
  PARAMETER,
}
