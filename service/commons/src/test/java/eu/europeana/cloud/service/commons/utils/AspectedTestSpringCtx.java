package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

public interface AspectedTestSpringCtx {

    String test_default(String someData);

    @Retryable(delay = 2000)
    void test_delay_2000();

    @Retryable(delay = 1000, maxAttempts = 2)
    void test_delay_1000_maxAttempts_2();

    @Retryable(delay = 1000, maxAttempts = 4)
    void test_delay_1000_maxAttempts_4();

    @Retryable(delay = 1000, errorMessage = "BIG_ERROR")
    void test_delay_1000_errorMessage_BIG_ERROR();
}
