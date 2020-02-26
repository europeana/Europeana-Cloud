package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.CassandraAuthenticationService;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationEntryPoint;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.event.LoggerListener;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

@Configuration
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, proxyTargetClass = true)  //<expression-handler ref="expressionHandler" /> ??
@EnableWebSecurity
//@Order(1)
public class AuthenticationConfiguration /*extends WebSecurityConfigurerAdapter*/ {

    private static final String JNDI_KEY_CASSANDRA_HOSTS = "java:comp/env/aas/cassandra/hosts";
    private static final String JNDI_KEY_CASSANDRA_PORT = "java:comp/env/aas/cassandra/port";
    private static final String JNDI_KEY_CASSANDRA_KEYSPACE = "java:comp/env/aas/cassandra/authentication-keyspace";
    private static final String JNDI_KEY_CASSANDRA_USERNAME = "java:comp/env/aas/cassandra/user";
    private static final String JNDI_KEY_CASSANDRA_PASSWORD = "java:comp/env/aas/cassandra/password";


    @Bean
    public CloudAuthenticationEntryPoint cloudAuthenticationEntryPoint() {
        return new CloudAuthenticationEntryPoint();
    }

    @Bean
    public CloudAuthenticationSuccessHandler cloudSecuritySuccessHandler() {
        return new CloudAuthenticationSuccessHandler();
    }

    @Bean
    public SimpleUrlAuthenticationFailureHandler cloudSecurityFailureHandler() {
        return new SimpleUrlAuthenticationFailureHandler();
    }

    //<http entry-point-ref="cloudAuthenticationEntryPoint" use-expressions="true" create-session="stateless">
   // @Override
    protected void _configure(HttpSecurity http) throws Exception {
        http.httpBasic()
                .authenticationEntryPoint(cloudAuthenticationEntryPoint())
                .and()
                .headers()
                .and()
                .formLogin()
                .successHandler(cloudSecuritySuccessHandler())
                .failureHandler(cloudSecurityFailureHandler());
    }

   // @Override
    protected void _configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.userDetailsService(authenticationService());
    }

    /* Automatically receives AuthenticationEvent messages */

    @Bean
    public LoggerListener loggerListener() {
        return new LoggerListener();
    }


    /* Delegates authorization to method calls. */


    @Bean
    public BCryptPasswordEncoder encoder() {
        return new BCryptPasswordEncoder();
    }



    /* ========= AUTHENTICATION STORAGE (USERNAME + PASSWORD TABLES IN CASSANDRA) ========= */

    @Bean
    public CassandraConnectionProvider provider() {
        String hosts = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_HOSTS);
        Integer port = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_PORT, Integer.class);
        String keyspaceName = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_KEYSPACE);
        String userName = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_USERNAME);
        String password = ServiceConfiguration.readJNDIValue(JNDI_KEY_CASSANDRA_PASSWORD);

        return new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
    }

    @Bean
    public CassandraUserDAO userDAO() {
        return new CassandraUserDAO(provider());
    }
    @Bean
    public CassandraAuthenticationService authenticationService() {
        return new CassandraAuthenticationService();
    }
}
