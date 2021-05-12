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
    private final static Logger LOGGER = LoggerFactory.getLogger(RetryAspect.class);

    @Pointcut("@within(eu.europeana.cloud.common.annotation.Retryable)")
    public void pointcutTypeWithRetryableAnn(){}

    @Pointcut("@annotation(eu.europeana.cloud.common.annotation.Retryable)")
    public void pointcutMethodWithRetryableAnn(){}

    @Pointcut("execution(* *(..))")
    public void pointcutExecuteAnyMethod(){}

    @Pointcut("pointcutTypeWithRetryableAnn() && pointcutMethodWithRetryableAnn() && pointcutExecuteAnyMethod()")
    public void pointcut(){}

    @Around("pointcut()")
    public Object retry(ProceedingJoinPoint proceedingJoinPoint) {
        Method method = ((MethodSignature) proceedingJoinPoint.getSignature()).getMethod();
        Retryable retryAnnotation = method.getAnnotation(Retryable.class);

        Object result = null;
        try {
            result = RetryableMethodExecutor.execute(retryAnnotation.errorMessage(),
                    retryAnnotation.maxAttempts(), retryAnnotation.delay(), () -> proceedingJoinPoint.proceed());
        }catch (Throwable t) {
            LOGGER.warn(RetryableMethodExecutor.createMessage(method, proceedingJoinPoint.getArgs()), t);
        }
        return result;
    }


}
