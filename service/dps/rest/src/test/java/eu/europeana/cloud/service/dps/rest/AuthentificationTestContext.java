package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.ObjectPostProcessor;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;

@Configuration
@Import({eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationEntryPoint.class,
        eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler.class,
        org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler.class,
        org.springframework.security.authentication.event.LoggerListener.class})
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, proxyTargetClass = true)
public class AuthentificationTestContext extends WebSecurityConfigurerAdapter {

    protected void configure(HttpSecurity http) throws Exception {
        http.
                httpBasic().and().
                sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS).and().
                csrf().disable();
    }

    @Autowired
    @Bean
    protected AuthenticationManager authenticationManager(ObjectPostProcessor objectPostProcessor) throws Exception {
        AuthenticationManagerBuilder auth=new AuthenticationManagerBuilder(objectPostProcessor);
        auth.inMemoryAuthentication().withUser("admin").password("{noop}admin").roles("ADMIN","USER")
                .and().withUser("Robin_Van_Persie").password("{noop}Feyenoord").roles("USER")
                .and().withUser("Cristiano").password("{noop}Ronaldo").roles("USER")
                .and().withUser("Anonymous").password("{noop}Anonymous").roles("ANONYMOUS");

       return auth.build();
    }

    @Bean
    public RecordExecutionSubmitService recordExecutionSubmitService() {
        return Mockito.mock(RecordExecutionSubmitService.class);
    }

//    @Bean
//    public PasswordEncoder passwordEncoder(){
//        return NoOpPasswordEncoder.getInstance();
//    }


    @Bean
    public ProcessedRecordsDAO processedRecordsDAO() {
        return Mockito.mock(ProcessedRecordsDAO.class);
    }

    @Bean
    public TaskStatusChecker taskStatusChecker() {
        return Mockito.mock(TaskStatusChecker.class);
    }

    @Bean
    public KafkaTopicSelector kafkaTopicSelector() {
        return Mockito.mock(KafkaTopicSelector.class);
    }

//    @Bean
//    public AuthenticationManager exposeManagerBean() throws Exception {
//        return authenticationManager();
//    }
}
