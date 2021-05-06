package eu.europeana.cloud.service.dps.storm.utils;

import javax.management.DescriptorKey;

public class TestDaoWithRetry {

    @DescriptorKey("Testing error method")
    public void retryableMethod() throws TestDaoExpection {
        throw new TestDaoExpection();
    }

    public void noRetryableMethod() throws TestDaoExpection {
        throw new TestDaoExpection();
    }

    @DescriptorKey("Testing error method")
    public void noErrorMethod() throws TestDaoExpection {
    }


}
