package eu.europeana.cloud.service.dps.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class DpsMetricService {
    private final Counter submittedTaskCounter;
    private final Counter killedTaskCounter;
    private final Counter restartedTaskCounter;
    private final Counter taskProgressCounter;

    public DpsMetricService(MeterRegistry meterRegistry) {
        submittedTaskCounter = Counter.builder("submitted_tasks")
                .description("Number of tasks submitted by DPS instance")
                .register(meterRegistry);
        killedTaskCounter = Counter.builder("killed_tasks")
                .description("Number of tasks killed by DPS instance")
                .register(meterRegistry);
        restartedTaskCounter = Counter.builder("restarted_tasks")
                .description("Number of tasks restarted by DPS instance")
                .register(meterRegistry);
        taskProgressCounter = Counter.builder("task_progress")
                .description("Number of tasks that progress was fetched by MCS instance")
                .register(meterRegistry);
    }

    public void incrementSubmittedTaskCounter() {
        submittedTaskCounter.increment();
    }

    public void incrementKilledTaskCounter() {
        killedTaskCounter.increment();
    }

    public void incrementRestartedTaskCounter() {
        restartedTaskCounter.increment();
    }

    public void incrementTaskProgressCounter() {
        taskProgressCounter.increment();
    }
}
