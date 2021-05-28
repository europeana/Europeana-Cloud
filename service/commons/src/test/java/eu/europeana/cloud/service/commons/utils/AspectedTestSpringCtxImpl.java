package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

@Retryable(delay = 1000)
public class AspectedTestSpringCtxImpl implements AspectedTestSpringCtx {
    public String test_default(String someData) {
        return String.format("Processed '%s'", someData);
    }

    public void test_delay_2000() {
    }

    public void test_delay_1000_maxAttempts_2() {
    }

    public void test_delay_1000_maxAttempts_4() {
    }

    public void test_delay_1000_errorMessage_BIG_ERROR() {
    }
}
