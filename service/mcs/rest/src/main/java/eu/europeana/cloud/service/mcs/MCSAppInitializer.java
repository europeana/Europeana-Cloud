package eu.europeana.cloud.service.mcs;

import eu.europeana.cloud.service.mcs.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.support.AbstractAnnotationConfigDispatcherServletInitializer;

public class MCSAppInitializer extends AbstractAnnotationConfigDispatcherServletInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MCSAppInitializer.class);

    @Override
    protected Class<?>[] getRootConfigClasses() {
        return null;
    }

    @Override
    protected Class<?>[] getServletConfigClasses() {
        LOGGER.info("MCS Rest Application starting...");
        return new Class<?>[]{
                ServiceConfiguration.class,
                AuthenticationConfiguration.class,
                AuthorizationConfiguration.class,
                UnitedExceptionMapper.class
        };
    }

    @Override
    protected String[] getServletMappings() {
        return new String[]{"/"};
    }
}
