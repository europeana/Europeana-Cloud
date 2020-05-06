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
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, proxyTargetClass = true)  //<expression-handler ref="expressionHandler" /> ??
public class AuthenticationConfiguration extends WebSecurityConfigurerAdapter {

    private static final String JNDI_KEY_CASSANDRA_HOSTS = "/aas/cassandra/hosts";
    private static final String JNDI_KEY_CASSANDRA_PORT = "/aas/cassandra/port";
    private static final String JNDI_KEY_CASSANDRA_KEYSPACE = "/aas/cassandra/authentication-keyspace";
    private static final String JNDI_KEY_CASSANDRA_USERNAME = "/aas/cassandra/user";
    private static final String JNDI_KEY_CASSANDRA_PASSWORD = "/aas/cassandra/password";

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

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.userDetailsService(authenticationService())
                .passwordEncoder(NoOpPasswordEncoder.getInstance());
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
