package eu.europeana.cloud.service.mcs.persistent.aspects;

import eu.europeana.cloud.service.mcs.persistent.exception.SwiftConnectionException;
import eu.europeana.cloud.service.mcs.persistent.swift.DynamicBlobStore;
import eu.europeana.cloud.service.mcs.persistent.swift.RetryOnFailure;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import javax.annotation.PostConstruct;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.jclouds.http.HttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Aspect that retries {@link DynamicBlobStore} method invocation in case
 * {@link HttpResponseException} of OpenStack Swift endpoint. Method that
 * retries must be annotated by interface {@link RetryOnFailure}.
 */
@Aspect
public class RetryOnTokenExpiryExecutor {

    @Autowired
    SwiftConnectionProvider provider;

    private int numberOfRetries;

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryOnTokenExpiryExecutor.class);


    @PostConstruct
    public void init() {
        numberOfRetries = 1;
    }


    @Pointcut("execution(* eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO.*(..))")
    private void isSwiftContentDAOFunction() {
    }


    @Around("isSwiftContentDAOFunction()")
    public Object retry(ProceedingJoinPoint pjp)
            throws Throwable {
        try {
            return pjp.proceed();
        } catch (SwiftConnectionException e) {
            return retryOnFailure((SwiftContentDAO) pjp.getTarget(), pjp.getSignature(), pjp.getArgs());
        }
    }


    private Object retryOnFailure(SwiftContentDAO contentDAO, Signature signature, Object[] args)
            throws NoSuchMethodException, IllegalAccessException, Throwable {
        for (int i = 0; i < numberOfRetries; i++) {
            //provider.reconnectConnections();
            try {
                final Method method = ((MethodSignature) signature).getMethod();
                Object o = method.invoke(contentDAO, args);
                return o;
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof SwiftConnectionException) {
                    LOGGER.info("Retry connection to proxy.");
                    continue;
                }
                throw e.getTargetException();
            }
        }
        throw new RuntimeException("All instances of Swift are down");
    }
}
