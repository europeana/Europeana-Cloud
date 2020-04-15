package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UnfinishedTasksContext {

    @Bean
    public TasksByStateDAO tasksDAO() {
        return Mockito.mock(TasksByStateDAO.class);
    }

    @Bean
    public String applicationIdentifier() {
        return "exampleAppIdentifier";
    }


}
