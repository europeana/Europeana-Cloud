package eu.europeana.cloud.service.aas.rest;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Class exposing the Application Context
 *
 * @author Markus.Muhr@theeuropeanlibrary.orgo
 */
public class ApplicationContextUtils implements ApplicationContextAware {

    /**
     * The Spring application context
     */
    private static ApplicationContext applicationContext;

    /**
     * @return The ApplicationContext instance
     */
    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        ApplicationContextUtils.applicationContext = applicationContext;
    }
}
