package data.validator.validator;

import data.validator.DataValidator;
import org.springframework.context.ApplicationContext;

/**
 * Created by Tarek on 5/2/2017.
 */
public class TableValidator implements Validator {
    @Override
    public void validate(ApplicationContext context, String sourceTableName, String targetTableName, int threadsCount) {
        System.out.println("Checking data integrity between source table " + sourceTableName + " and target table " + targetTableName);
        DataValidator dataValidator = (DataValidator) context.getBean("dataValidator");
        dataValidator.validate(sourceTableName, targetTableName, threadsCount);
    }
}
