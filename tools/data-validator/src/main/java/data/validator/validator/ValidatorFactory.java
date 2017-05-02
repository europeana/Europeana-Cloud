package data.validator.validator;

import data.validator.constants.ValidatorType;

/**
 * Created by Tarek on 5/2/2017.
 */
public class ValidatorFactory {

    public static Validator getValidator(ValidatorType type) {
        if (type == ValidatorType.KEYSPACE)
            return new KeyspaceValidator();
        if (type == ValidatorType.TABLE)
            return new TableValidator();
        return null;
    }

}
