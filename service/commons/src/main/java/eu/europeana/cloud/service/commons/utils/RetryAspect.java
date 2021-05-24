package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;

@Aspect
public class RetryAspect {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryAspect.class);

    @Pointcut("@within(eu.europeana.cloud.common.annotation.Retryable)")
    public void pointcutTypeWithRetryableAnn(){
        //Dummy method for defining cut point for AOP (see annotation above)
    }

    @Pointcut("@annotation(eu.europeana.cloud.common.annotation.Retryable)")
    public void pointcutMethodWithRetryableAnn(){
        //Dummy method for defining cut point for AOP (see annotation above)
    }

    @Pointcut("pointcutTypeWithRetryableAnn() || pointcutMethodWithRetryableAnn()")
    public void pointcut(){
        //Dummy method for defining cut point for AOP (see annotation above)
    }

    @Around("pointcut()")
    public Object retry(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        Method method = ((MethodSignature) proceedingJoinPoint.getSignature()).getMethod();

        LOGGER.debug("Retry aspect called for '{}'", method);

        Retryable retryAnnotation = getAnnotationForMethodOrClass(proceedingJoinPoint);

        String errorMessage =
                RetryableMethodExecutor.createMessage(method, retryAnnotation, proceedingJoinPoint.getArgs());

        return RetryableMethodExecutor.execute(errorMessage,
                retryAnnotation.maxAttempts(), retryAnnotation.delay(), proceedingJoinPoint::proceed);
    }

    private Retryable getAnnotationForMethodOrClass(ProceedingJoinPoint proceedingJoinPoint) {
        Method method = ((MethodSignature) proceedingJoinPoint.getSignature()).getMethod();

        //Get annotation from method (directly or from interface)
        Retryable retryAnnotation = AnnotationUtils.findAnnotation(method, Retryable.class);

        //If no annotation, get it from target type
        if(retryAnnotation == null) {
            retryAnnotation = proceedingJoinPoint.getTarget().getClass().getAnnotation(Retryable.class);
        }

        return retryAnnotation;
    }
}
