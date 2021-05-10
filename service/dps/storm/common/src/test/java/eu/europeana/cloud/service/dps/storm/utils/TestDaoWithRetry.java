package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.annotations.Retryable;

public class TestDaoWithRetry {

    @Retryable(delay = 100, maxAttempts = 2)
    public void retryableMethod() throws TestDaoExpection {
        throw new TestDaoExpection();
    }

    public void noRetryableMethod() throws TestDaoExpection {
        throw new TestDaoExpection();
    }

    @Retryable(delay = 100, maxAttempts = 2)
    public void noErrorMethod() throws TestDaoExpection {
    }


}
