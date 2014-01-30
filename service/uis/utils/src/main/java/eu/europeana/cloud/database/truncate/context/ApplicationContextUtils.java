package eu.europeana.cloud.database.truncate.context;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Class exposing the Application Context
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class ApplicationContextUtils implements ApplicationContextAware {
	/**
	 * The Spring application context
	 */
    private static ApplicationContext applicationContext =  new ClassPathXmlApplicationContext("context.xml");

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