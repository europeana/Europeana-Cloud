package eu.europeana.cloud.service.mcs.persistent.aspects;

import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

/**
 * Aspect that translates every runtime exception thrown in services into {@link SystemException} that indicates any
 * problem with another server (Cassandra, Swift, Solr).
 */
@Aspect
public class ServiceExceptionTranslator {

    /**
     * Pointcut for class annotated with spring Service annotation
     */
    @Pointcut("@within(org.springframework.stereotype.Service)")
    private void isService() {
    }


    /**
     * Pointcut for our service implementation package
     */
    @Pointcut("within(eu.europeana.cloud.service.mcs.persistent..*)")
    private void inMCSPersistentPackage() {
    }


    @AfterThrowing(pointcut = "isService() && inMCSPersistentPackage()", throwing = "ex")
    public void wrapException(RuntimeException ex) {
        // if exception is already our generic system exception - let it be and do nothing
        if (ex instanceof SystemException) {
            return;
        }
        // else - wrap it into our exception
        SystemException wrappedException = new SystemException(ex.getMessage(), ex);
        wrappedException.setStackTrace(ex.getStackTrace());
        throw wrappedException;
    }
}
