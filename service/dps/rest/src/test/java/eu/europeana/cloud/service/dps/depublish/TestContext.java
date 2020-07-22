package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.cloud.service.dps.storm.utils.RecordStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

@Configuration
@EnableAsync
public class TestContext {

    @Bean
    public TaskStatusChecker taskStatusChecker() {
        return Mockito.mock(TaskStatusChecker.class);
    }

    @Bean
    public TaskStatusUpdater taskStatusUpdater() {
        return Mockito.mock(TaskStatusUpdater.class);
    }

    @Bean
    public MetisIndexerFactory metisIndexerFactory() {
        return Mockito.mock(MetisIndexerFactory.class);
    }

    @Bean
    public RecordStatusUpdater recordStatusUpdater() {
        return Mockito.mock(RecordStatusUpdater.class);
    }
}
