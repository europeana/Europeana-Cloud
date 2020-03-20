package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.service.dps.config.AuthenticationConfiguration;
import eu.europeana.cloud.service.dps.config.AuthorizationConfiguration;
import eu.europeana.cloud.service.dps.config.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.support.AbstractAnnotationConfigDispatcherServletInitializer;

public class DPSAppInitializer extends AbstractAnnotationConfigDispatcherServletInitializer {

    private final static Logger LOGGER = LoggerFactory.getLogger(DPSAppInitializer.class);

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
