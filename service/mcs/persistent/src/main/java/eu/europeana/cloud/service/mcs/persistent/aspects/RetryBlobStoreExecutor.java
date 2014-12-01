package eu.europeana.cloud.service.mcs.persistent.aspects;

import eu.europeana.cloud.service.mcs.persistent.swift.DBlobStore;
import eu.europeana.cloud.service.mcs.persistent.swift.DynamicBlobStore;
import eu.europeana.cloud.service.mcs.persistent.swift.RetryOnFailure;
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
public class RetryBlobStoreExecutor {

    @Autowired
    DBlobStore dynamicBlobStore;

    private int numberOfRetries;

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(RetryBlobStoreExecutor.class);

    @PostConstruct
    public void init() {
	numberOfRetries = dynamicBlobStore.getInstanceNumber() - 1;
    }

    @Pointcut("execution(* eu.europeana.cloud.service.mcs.persistent.swift.DynamicBlobStore.*(..))")
    private void isDynamicBlobStoreFunction() {
    }

    @Pointcut("@annotation(eu.europeana.cloud.service.mcs.persistent.swift.RetryOnFailure)")
    private void isMarkedAsRetryOnFailure() {
    }

    @Around("isDynamicBlobStoreFunction() && isMarkedAsRetryOnFailure()")
    public Object retry(ProceedingJoinPoint pjp) throws Throwable {
	try {
	    return pjp.proceed();
	} catch (HttpResponseException e) {
	    DynamicBlobStore failureBlobStore = dynamicBlobStore
		    .getDynamicBlobStoreWithoutActiveInstance();
	    return retryOnFailure(failureBlobStore, pjp.getSignature(),
		    pjp.getArgs());
	}
    }

    private Object retryOnFailure(DynamicBlobStore failureBlobStore,
	    Signature signature, Object[] args) throws NoSuchMethodException,
	    IllegalAccessException, Throwable {
	for (int i = 0; i < numberOfRetries; i++) {
	    try {
		final Method method = ((MethodSignature) signature).getMethod();
		Object o = method.invoke(failureBlobStore, args);
		return o;
	    } catch (InvocationTargetException e) {
		if (e.getTargetException() instanceof HttpResponseException) {
		    LOGGER.info("Failrue of the proxy switch in to next.");
		    continue;
		}
		throw e.getTargetException();
	    }
	}
	throw new RuntimeException("All instances of Swift are down");
    }
}
