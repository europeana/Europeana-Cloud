package eu.europeana.cloud.service.mcs.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class McsMetricService {
    private final Counter getRecordCounter;
    private final Counter deleteRecordCounter;

    public McsMetricService(MeterRegistry meterRegistry) {
        getRecordCounter = Counter.builder("records_retrieved")
                .description("Number of records retrieved created by MCS instance")
                .register(meterRegistry);
        deleteRecordCounter = Counter.builder("records_deleted")
                .description("Number of records deleted by MCS instance")
                .register(meterRegistry);
    }

    public void incrementGetRecordCounter() {
        getRecordCounter.increment();
    }

    public void incrementDeleteRecordCounter() {
        deleteRecordCounter.increment();
    }
}
