package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import java.lang.reflect.Method;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;

@Aspect
public class RetryAspect {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryAspect.class);

  @Pointcut("@within(eu.europeana.cloud.common.annotation.Retryable)")
  public void pointcutTypeWithRetryableAnn() {
    //Dummy method for defining cut point for AOP (see annotation above)
  }

  @Pointcut("@annotation(eu.europeana.cloud.common.annotation.Retryable)")
  public void pointcutMethodWithRetryableAnn() {
    //Dummy method for defining cut point for AOP (see annotation above)
  }

  @Pointcut("pointcutTypeWithRetryableAnn() || pointcutMethodWithRetryableAnn()")
  public void pointcut() {
    //Dummy method for defining cut point for AOP (see annotation above)
  }

  @Around("pointcut()")
  public Object retry(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
    LOGGER.trace("Retry aspect called for '{}'", getMethod(proceedingJoinPoint));

    Retryable retryAnnotation = getAnnotationForMethodOrClass(proceedingJoinPoint);

    String errorMessage =
        RetryableMethodExecutor.createMessage(getMethod(proceedingJoinPoint), retryAnnotation, proceedingJoinPoint.getArgs());

    return RetryableMethodExecutor.execute(errorMessage,
        retryAnnotation.maxAttempts(), retryAnnotation.delay(), proceedingJoinPoint::proceed);
  }

  private Method getMethod(ProceedingJoinPoint proceedingJoinPoint) {
    return ((MethodSignature) proceedingJoinPoint.getSignature()).getMethod();
  }

  private Retryable getAnnotationForMethodOrClass(ProceedingJoinPoint proceedingJoinPoint) {
    //Get annotation from method (directly or from interface)
    Retryable retryAnnotation = AnnotationUtils.findAnnotation(getMethod(proceedingJoinPoint), Retryable.class);

    //If we got interface, but
    if (retryAnnotation == null) {
      try {
        Method m = proceedingJoinPoint.getTarget().getClass().getMethod(
            getMethod(proceedingJoinPoint).getName(), getMethod(proceedingJoinPoint).getParameterTypes());
        retryAnnotation = m.getAnnotation(Retryable.class);
      } catch (NoSuchMethodException nsm) {
        //Skip the exception. Case impossible
      }
    }

    //If no annotation, get it from target type
    if (retryAnnotation == null) {
      retryAnnotation = proceedingJoinPoint.getTarget().getClass().getAnnotation(Retryable.class);
    }

    return retryAnnotation;
  }
}
