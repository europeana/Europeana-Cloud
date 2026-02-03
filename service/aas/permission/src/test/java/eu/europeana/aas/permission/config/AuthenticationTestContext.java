package eu.europeana.aas.permission.config;

import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationEntryPoint;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.authentication.event.LoggerListener;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true, securedEnabled = true)
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
    @SuppressWarnings("deprecation")
    public PasswordEncoder passwordEncoder() {
        return NoOpPasswordEncoder.getInstance();
    }

    @Bean
    public UserDetailsService userDetailsService() {
        return new InMemoryUserDetailsManager(
                User.withUsername("admin").password("admin").roles("ADMIN", "USER").build(),
                User.withUsername("Robin_Van_Persie").password("Feyenoord").roles("USER").build(),
                User.withUsername("Cristiano").password("Ronaldo").roles("USER").build(),
                User.withUsername("Anonymous").password("Anonymous").roles("ANONYMOUS").build()
        );
    }

    @Bean("authenticationManager")
    public AuthenticationManager authenticationManager(
            UserDetailsService uds,
            PasswordEncoder encoder) {

        DaoAuthenticationProvider provider = new DaoAuthenticationProvider(uds);
        provider.setPasswordEncoder(encoder);
        return new ProviderManager(provider);
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) {
        http
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .exceptionHandling(e -> e.authenticationEntryPoint(cloudAuthenticationEntryPoint()))
                .authorizeHttpRequests(a -> a.anyRequest().permitAll())
                .httpBasic(Customizer.withDefaults())
                .headers(Customizer.withDefaults())
                .formLogin(f -> f
                        .successHandler(cloudSecuritySuccessHandler())
                        .failureHandler(cloudSecurityFailureHandler())
                );

        return http.build();
    }

}
