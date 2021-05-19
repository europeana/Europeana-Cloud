package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AspectedTest2Impl implements AspectedTest2Interface {
    private static final Logger LOGGER = LoggerFactory.getLogger(AspectedTest2Impl.class);
    private int currentAttampt = 0;

    public AspectedTest2Impl() {
    }

    @Retryable(delay = 1000)
    public String testMethod05_fails_3(String s1, int i2) {
        currentAttampt++;
        if(currentAttampt <= 3) {
            LOGGER.info("Failed attempt number {}", currentAttampt);
            throw new TestRuntimeExpection();
        }
        return String.format("%s : %d", s1, i2) ;
    }

}
