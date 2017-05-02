package data.validator.validator;

import org.springframework.context.ApplicationContext;

import java.util.concurrent.ExecutionException;

/**
 * Created by Tarek on 5/2/2017.
 */
public interface Validator {
    void validate(ApplicationContext context, String sourceTableName, String targetTableName, int threadsCount) throws InterruptedException,ExecutionException;
}
