package eu.europeana.cloud.service.uis.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class UisMetricService {
    private final Counter cloudIdsCreatedCounter;
    private final Counter cloudIdsRetrievedCounter;
    private final Counter localIdsRetrievedCounter;

    public UisMetricService(MeterRegistry meterRegistry) {
        cloudIdsCreatedCounter = Counter.builder("cloud_ids_created")
                .description("Number of cloud IDs created by UIS instance")
                .tags("app", "uis")
                .register(meterRegistry);
        cloudIdsRetrievedCounter = Counter.builder("cloud_ids_retrieved")
                .description("Number of times cloud ID was being retrieved by UIS instance")
                .tags("app", "uis")
                .register(meterRegistry);
        localIdsRetrievedCounter = Counter.builder("local_ids_retrieved")
                .description("Number of times local ID was being retrieved by UIS instance")
                .tags("app", "uis")
                .register(meterRegistry);
    }

    public void incrementCloudIdsCreatedCounter() {
        cloudIdsCreatedCounter.increment();
    }

    public void incrementCloudIdsRetrievedCounter() {
        cloudIdsRetrievedCounter.increment();
    }

    public void incrementLocalIdsRetrievedCounter() {
        localIdsRetrievedCounter.increment();
    }
}
