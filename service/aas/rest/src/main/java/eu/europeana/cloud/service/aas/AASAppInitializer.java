package eu.europeana.cloud.service.aas;


import eu.europeana.cloud.service.aas.config.AuthenticationConfiguration;
import eu.europeana.cloud.service.aas.config.AuthorizationConfiguration;
import eu.europeana.cloud.service.aas.config.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.support.AbstractAnnotationConfigDispatcherServletInitializer;

public class AASAppInitializer extends AbstractAnnotationConfigDispatcherServletInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AASAppInitializer.class);

    @Override
    protected Class<?>[] getRootConfigClasses() {
        return null;
    }

    @Override
    protected Class<?>[] getServletConfigClasses() {
        LOGGER.info("DPS Rest Application starting...");
        return new Class<?>[]{
                ServiceConfiguration.class,
                AuthenticationConfiguration.class,
                AuthorizationConfiguration.class};
    }

    @Override
    protected String[] getServletMappings() {
        return new String[]{"/"};
    }
}
