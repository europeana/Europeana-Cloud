package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Retryable(delay = 1000)
public class AspectedTest1Impl implements AspectedTest1Interface {
    private static final Logger LOGGER = LoggerFactory.getLogger(AspectedTest1Impl.class);

    protected int currentAttempt = 0;

    @Override
    public String testMethod01_fails_2(String s1, int i2) {
        currentAttempt++;
        if(currentAttempt <= 2) {
            LOGGER.info("Failed attempt number {}", currentAttempt);
            throw new TestRuntimeExpection();
        }
        return String.format("%s : %d", s1, i2) ;
    }

    @Override
    public void testMethod02_fails_4(Object p1, Object p2) {
        currentAttempt++;
        if(currentAttempt <= 4) {
            LOGGER.info("Failed attempt number {} with parameters p1 = {}; p2 = {}", currentAttempt, p1, p2);
            throw new TestRuntimeExpection();
        }
   }

    @Override
    @Retryable(maxAttempts = 2, delay = 20*1000)  //overwrite default values
    public String testMethod03_fails_1() {
        currentAttempt++;
        if(currentAttempt <= 1) {
            LOGGER.info("Failed attempt number {}", currentAttempt);
            throw new TestRuntimeExpection();
        }
        return "SUCCESS";
    }

    @Override
    @Retryable(maxAttempts = 2, delay = 1000) //overwrite default values
    public String testMethod04_fails_3() {
        currentAttempt++;
        if(currentAttempt <= 3) {
            LOGGER.info("Failed attempt number {}", currentAttempt);
            throw new TestRuntimeExpection();
        }
        return "SUCCESS";
    }

}
