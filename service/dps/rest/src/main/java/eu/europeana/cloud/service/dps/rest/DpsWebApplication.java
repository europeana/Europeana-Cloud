package eu.europeana.cloud.service.dps.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletRegistration;

@ComponentScan({ "eu.europeana.cloud.service.dps.rest" })
@Order(Ordered.HIGHEST_PRECEDENCE)
public class DpsWebApplication implements WebApplicationInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestsExecutor.class);

    @Override
    public void onStartup(ServletContext container) {
        LOGGER.info("DPS Rest Application starting...");

        AnnotationConfigWebApplicationContext context
                = new AnnotationConfigWebApplicationContext();
        context.setConfigLocation("eu.europeana.cloud.service.dps.rest");

        container.addListener(new ContextLoaderListener(context));

        ServletRegistration.Dynamic dispatcher = container
                .addServlet("dispatcher", new DispatcherServlet(context));

        dispatcher.setLoadOnStartup(1);
        dispatcher.addMapping("/");
    }
}
