package com.syssniper.jsonschemakvstore.config;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;


@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain defaultSecurityFilterChain(HttpSecurity http) throws Exception {
         http
                 .csrf()
                 .disable()
                 .authorizeHttpRequests()
                 .anyRequest()
                 .authenticated()
                 .and()
                 .oauth2Login()
                 .and()
                 .oauth2ResourceServer().jwt();
        return http.build();
    }
}
