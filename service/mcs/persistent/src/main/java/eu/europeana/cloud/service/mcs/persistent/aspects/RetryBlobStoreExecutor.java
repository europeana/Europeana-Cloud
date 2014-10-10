package eu.europeana.cloud.service.mcs.persistent.aspects;

import eu.europeana.cloud.service.mcs.persistent.swift.DynamicBlobStore;
import eu.europeana.cloud.service.mcs.persistent.swift.RetryOnFailure;
import javax.annotation.PostConstruct;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
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
    DynamicBlobStore dynamicBlobStore;

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(RetryBlobStoreExecutor.class);

    private int numberOfExecution;

    @PostConstruct
    public void init() {
	numberOfExecution = dynamicBlobStore.getInstanceNumber();
    }

    @Pointcut("execution(* eu.europeana.cloud.service.mcs.persistent.swift.DynamicBlobStore.*(..))")
    private void isDynamicBlobStoreFunction() {
    }

    @Pointcut("@annotation(eu.europeana.cloud.service.mcs.persistent.swift.RetryOnFailure)")
    private void isMarkedAsRetryOnFailure() {
    }

    @Around("isDynamicBlobStoreFunction() && isMarkedAsRetryOnFailure()")
    public Object retry(ProceedingJoinPoint pjp) throws Throwable {
	int executionCount = 1;
	while (true) {
	    try {
		return pjp.proceed();
	    } catch (HttpResponseException e) {

		if (++executionCount > numberOfExecution) {
		    throw new RuntimeException(
			    "All instances of Swift are down");
		}
		dynamicBlobStore.switchInstance();
	    }
	}
    }
}
