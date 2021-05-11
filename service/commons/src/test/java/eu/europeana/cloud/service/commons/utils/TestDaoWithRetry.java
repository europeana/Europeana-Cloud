package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

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
