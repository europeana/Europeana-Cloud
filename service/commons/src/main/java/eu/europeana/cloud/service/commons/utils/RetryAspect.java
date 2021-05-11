package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

import java.io.IOException;
import java.lang.reflect.Method;

@Aspect
public class RetryAspect {
    @Pointcut("execution(* eu.europeana.cloud.service.uis.dao.*(..)) && @annotation(eu.europeana.cloud.common.annotation.Retryable)")
    public void pointcut(){}

    @Around("pointcut()")
    public Object retry(ProceedingJoinPoint proceedingJoinPoint) {
        Method method = ((MethodSignature) proceedingJoinPoint.getSignature()).getMethod();
        Retryable retryAnnotation = method.getAnnotation(Retryable.class);

        //Object[] args = proceedingJoinPoint.getArgs();

        Object result = null;
        try {
            result = RetryableMethodExecutor.execute(retryAnnotation.errorMessage(), retryAnnotation.maxAttempts(), retryAnnotation.delay(), () -> proceedingJoinPoint.proceed());
        }catch (Throwable t) {
            t.printStackTrace();
        }

        return result;
    }


}
