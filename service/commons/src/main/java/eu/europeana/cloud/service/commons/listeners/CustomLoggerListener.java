package eu.europeana.cloud.service.commons.listeners;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.core.log.LogMessage;
import org.springframework.security.authentication.event.AbstractAuthenticationEvent;
import org.springframework.security.authentication.event.AbstractAuthenticationFailureEvent;
import org.springframework.security.authentication.event.AuthenticationSuccessEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

@Component
public class CustomLoggerListener implements ApplicationListener<AbstractAuthenticationEvent> {
    private static final Log logger = LogFactory.getLog(CustomLoggerListener.class);

    public CustomLoggerListener() {
    }

    @Override
    public void onApplicationEvent(AbstractAuthenticationEvent event) {
        if (event instanceof AuthenticationSuccessEvent) {
            logger.trace(LogMessage.of(() ->
                this.getSuccessfulAuthenticationMessage(event)
            ));
        } else {
            logger.debug(LogMessage.of(() ->
                 this.getUnsuccessfulAuthenticationMessage(event)
            ));
        }
    }

    private String getSuccessfulAuthenticationMessage(AbstractAuthenticationEvent event) {
        StringBuilder builder = new StringBuilder();
        builder.append("Authentication event ");
        builder.append(ClassUtils.getShortName(event.getClass()));
        builder.append(": ");
        builder.append(event.getAuthentication().getName());
        return builder.toString();
    }

    private String getUnsuccessfulAuthenticationMessage(AbstractAuthenticationEvent event) {
        StringBuilder builder = new StringBuilder();
        builder.append("Authentication event ");
        builder.append(ClassUtils.getShortName(event.getClass()));
        builder.append(": ");
        builder.append(event.getAuthentication().getName());
        builder.append("; details: ");
        builder.append(event.getAuthentication().getDetails());
        if (event instanceof AbstractAuthenticationFailureEvent) {
            builder.append("; exception: ");
            builder.append(((AbstractAuthenticationFailureEvent)event).getException().getMessage());
        }

        return builder.toString();
    }
}
