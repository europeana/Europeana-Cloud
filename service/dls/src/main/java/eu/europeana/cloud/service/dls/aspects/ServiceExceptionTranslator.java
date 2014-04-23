package eu.europeana.cloud.service.dls.aspects;

import eu.europeana.cloud.service.dls.solr.exception.SystemException;
import java.io.IOException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

@Aspect
public class ServiceExceptionTranslator {

    /**
     * Pointcut for class annotated with spring Repository annotation
     */
    @Pointcut("@within(org.springframework.stereotype.Repository)")
    private void isRepository() {
    }


    /**
     * Pointcut for our service implementation package
     */
    @Pointcut("within(eu.europeana.cloud.service.dls.solr..*)")
    private void inDLSSolrPackage() {
    }


    /**
     * Wraps every {@link SolrException}, {@link SolrServerException}, {@link IOException} exception thrown by system into {@link SystemException}.
     * 
     * @param exception
     *            exception to wrap
     */
    @AfterThrowing(pointcut = "isRepository() && inDLSSolrPackage()", throwing = "exception")
    public void wrapException(final Exception exception) {
        if (exception instanceof SolrException | exception instanceof SolrServerException
                | exception instanceof IOException) {
            final SystemException wrappedException = new SystemException(exception.getMessage(), exception);
            wrappedException.setStackTrace(exception.getStackTrace());
            throw wrappedException;
        }
        //other exceptions simple do nothing
    }
}