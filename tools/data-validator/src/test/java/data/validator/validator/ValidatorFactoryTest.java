package data.validator.validator;

import data.validator.constants.ValidatorType;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by Tarek on 5/17/2017.
 */
public class ValidatorFactoryTest {

  @Test
  public void TestValidator() {
    Validator validator = ValidatorFactory.getValidator(ValidatorType.KEYSPACE);
    assertTrue(validator instanceof KeyspaceValidator);
    validator = ValidatorFactory.getValidator(ValidatorType.TABLE);
    assertTrue(validator instanceof TableValidator);

  }
}
