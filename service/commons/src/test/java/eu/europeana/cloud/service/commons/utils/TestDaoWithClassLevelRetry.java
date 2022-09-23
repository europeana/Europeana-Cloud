package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

@Retryable(delay = 100, maxAttempts = 3)
public class TestDaoWithClassLevelRetry {

    public void methodWithoutRetryableAnnotation() throws TestRuntimeExpection {
        throw new TestRuntimeExpection();
    }

    @Retryable(delay = 100, maxAttempts = 1)
    public void methodWithOverridedRetryableAnnotation() throws TestRuntimeExpection {
        throw new TestRuntimeExpection();
    }

}
