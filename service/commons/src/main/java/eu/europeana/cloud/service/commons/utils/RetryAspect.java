package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Pointcut("execution(* *(..))")
    public void pointcutExecuteAnyMethod(){
        //Dummy method for defining cut point for AOP (see annotation above)
    }

    @Pointcut("pointcutExecuteAnyMethod() && (pointcutTypeWithRetryableAnn() || pointcutMethodWithRetryableAnn())")
    public void pointcut(){
        //Dummy method for defining cut point for AOP (see annotation above)
    }

    @Around("pointcut()")
    public Object retry(ProceedingJoinPoint proceedingJoinPoint) {
        Method method = ((MethodSignature) proceedingJoinPoint.getSignature()).getMethod();
        Retryable retryAnnotation = method.getAnnotation(Retryable.class);

        if(retryAnnotation == null) {
            retryAnnotation = proceedingJoinPoint.getTarget().getClass().getAnnotation(Retryable.class);
        }

        Object result = null;
        try {
            result = RetryableMethodExecutor.execute(retryAnnotation.errorMessage(),
                    retryAnnotation.maxAttempts(), retryAnnotation.delay(), proceedingJoinPoint::proceed);
        }catch (Throwable t) {
            LOGGER.warn(RetryableMethodExecutor.createMessage(method, proceedingJoinPoint.getArgs()), t);
        }
        return result;
    }
}
