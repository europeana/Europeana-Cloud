package eu.europeana.cloud.service.uis.config;

import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationEntryPoint;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.event.LoggerListener;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

@Configuration
@EnableMethodSecurity(prePostEnabled = true)
@EnableWebSecurity
public class AuthenticationTestContext {

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

    @Bean
    public LoggerListener loggerListener() {
        return new LoggerListener();
    }

    @Bean
    public PasswordEncoder encoder() {
        return NoOpPasswordEncoder.getInstance();
    }

    @Bean
    public InMemoryUserDetailsManager userDetailsService() {
        UserDetails admin = User.withUsername("admin")
                .password("admin")
                .roles("ADMIN")
                .build();

        UserDetails robin = User.withUsername("Robin_Van_Persie")
                .password("Feyenoord")
                .roles("USER")
                .build();

        UserDetails cristiano = User.withUsername("Cristiano")
                .password("Ronaldo")
                .roles("USER")
                .build();

        return new InMemoryUserDetailsManager(admin, robin, cristiano);
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration config) {
        return config.getAuthenticationManager();
    }

    @Bean
    public SecurityFilterChain filterChain(
            HttpSecurity http,
            CloudAuthenticationEntryPoint entryPoint,
            CloudAuthenticationSuccessHandler successHandler,
            SimpleUrlAuthenticationFailureHandler failureHandler
    ) {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(sess -> sess.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
                .httpBasic(httpBasic -> {
                })
                .formLogin(form -> form
                        .successHandler(successHandler)
                        .failureHandler(failureHandler))
                .exceptionHandling(ex -> ex.authenticationEntryPoint(entryPoint))
                .headers(headers -> {
                });

        return http.build();
    }
}
