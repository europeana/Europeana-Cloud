package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.CassandraAuthenticationService;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationEntryPoint;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.authentication.event.LoggerListener;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, proxyTargetClass = true)  //<expression-handler ref="expressionHandler" /> ??
public class AuthenticationConfiguration extends WebSecurityConfigurerAdapter {

    private final static String JNDI_KEY_CASSANDRA_HOSTS = "/aas/cassandra/hosts";
    private final static String JNDI_KEY_CASSANDRA_PORT = "/aas/cassandra/port";
    private final static String JNDI_KEY_CASSANDRA_KEYSPACE = "/aas/cassandra/authentication-keyspace";
    private final static String JNDI_KEY_CASSANDRA_USERNAME = "/aas/cassandra/user";
    private final static String JNDI_KEY_CASSANDRA_PASSWORD = "/aas/cassandra/password";

    @Autowired
    private Environment environment;

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.httpBasic()
                .and()
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                .and()
                .csrf().disable();
    }

    @Bean
    public CloudAuthenticationEntryPoint cloudAuthenticationEntryPoint() {
        return new CloudAuthenticationEntryPoint();
    }


    @Bean
    public LoggerListener loggerListener() {
        return new LoggerListener();
    }

    // ???
    @Bean
    public BCryptPasswordEncoder encoder() {
        return new BCryptPasswordEncoder();
    }

    /* ========= AUTHENTICATION STORAGE (USERNAME + PASSWORD TABLES IN CASSANDRA) ========= */

    @Bean
    public CassandraConnectionProvider provider() {
        String hosts = environment.getProperty(JNDI_KEY_CASSANDRA_HOSTS);
        Integer port = environment.getProperty(JNDI_KEY_CASSANDRA_PORT, Integer.class);
        String keyspaceName = environment.getProperty(JNDI_KEY_CASSANDRA_KEYSPACE);
        String userName = environment.getProperty(JNDI_KEY_CASSANDRA_USERNAME);
        String password = environment.getProperty(JNDI_KEY_CASSANDRA_PASSWORD);

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
