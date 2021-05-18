package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Retryable
public class AspectTestImpl implements AspectTestInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(AspectTestImpl.class);

    private static final int DEFAULT_ATTERMPTS_COUNTER = 3;
    private Map<String, Integer> failAttemptsCounters;
    private int currentAttampt;

    public AspectTestImpl() {
        this(null);
    }

    public AspectTestImpl(Map<String, Integer> failAttemptsCounters) {
        this.failAttemptsCounters = failAttemptsCounters;
        resetCounter();
    }

    public void resetCounter() {
        currentAttampt = 0;
    }

    @Override
    @Retryable(delay = 1000)
    public String testMethod01(String s1, int i2) {
        int attempts = getAttemptsCounterForMethod();
        currentAttampt++;
        if(currentAttampt <= attempts) {
            LOGGER.info("Failed attempt number {}", currentAttampt);
            throw new TestRuntimeExpection();
        }
        return String.format("%s : %d", s1, i2) ;
    }

    @Override
    @Retryable(delay = 1000)
    public void testMethod02(Object p1, Object p2) {
        int attempts = getAttemptsCounterForMethod();
        currentAttampt++;
        if(currentAttampt <= attempts) {
            LOGGER.info("Failed attempt number {} with parameters p1 = {}; p2 = {}", currentAttampt, p1, p2);
            throw new TestRuntimeExpection();
        }
   }

    @Override
    @Retryable(maxAttempts = 2, delay = 20*1000)
    public String testMethod03() {
        int attempts = getAttemptsCounterForMethod();
        currentAttampt++;
        if(currentAttampt <= attempts) {
            LOGGER.info("Failed attempt number {}", currentAttampt);
            throw new TestRuntimeExpection();
        }
        return "SUCCESS";
    }

    @Override
    @Retryable(maxAttempts = 2, delay = 1000)
    public String testMethod04() {
        int attempts = getAttemptsCounterForMethod();
        currentAttampt++;
        if(currentAttampt <= attempts) {
            LOGGER.info("Failed attempt number {}", currentAttampt);
            throw new TestRuntimeExpection();
        }
        return "SUCCESS";
    }


    private int getAttemptsCounterForMethod() {
        if(failAttemptsCounters != null &&
                failAttemptsCounters.containsKey(Thread.currentThread().getStackTrace()[2].getMethodName())) {
            return failAttemptsCounters.get(Thread.currentThread().getStackTrace()[2].getMethodName());
        } else {
            return DEFAULT_ATTERMPTS_COUNTER;
        }
    }
}
