package eu.europeana.cloud.service.aas.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.CassandraAuthenticationService;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.event.LoggerListener;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, proxyTargetClass = true)
public class AuthenticationConfiguration extends WebSecurityConfigurerAdapter {

    @Bean
    public CassandraUserDAO userDAO(CassandraConnectionProvider aasCassandraProvider) {
        return new CassandraUserDAO(aasCassandraProvider);
    }

    @Bean
    public CassandraAuthenticationService authenticationService() {
        return new CassandraAuthenticationService();
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.userDetailsService(authenticationService())
                .passwordEncoder(passwordEncoder());
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        try {
            return new BCryptPasswordEncoder(4, SecureRandom.getInstance("PKCS11"));
        } catch (NoSuchAlgorithmException noSuchAlgorithmException) {
            return new BCryptPasswordEncoder(4);
        }
    }

    @Bean
    public LoggerListener loggerListener() {
        return new LoggerListener();
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.
                httpBasic().and().
                sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS).and().
                csrf().disable();
    }

}
