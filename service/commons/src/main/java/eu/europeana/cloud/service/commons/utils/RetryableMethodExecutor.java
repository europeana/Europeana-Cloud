package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType.Unloaded;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryableMethodExecutor {


  private static final String ATTEMPT_COUNT_PROPERTY_NAME = "ECLOUD_OVERRIDE_RETRIES_ATTEMPT_COUNT";
  public static final Integer OVERRIDE_ATTEMPT_COUNT = getSystemPropertyOrEnvVariable(ATTEMPT_COUNT_PROPERTY_NAME);
  private static final String DELAY_VALUE_PROPERTY_NAME = "ECLOUD_OVERRIDE_RETRIES_DELAY";
  public static final Integer OVERRIDE_DELAY_BETWEEN_ATTEMPTS = getSystemPropertyOrEnvVariable(DELAY_VALUE_PROPERTY_NAME);
  private static final Logger LOGGER = LoggerFactory.getLogger(RetryableMethodExecutor.class);

  public static final int DEFAULT_REST_ATTEMPTS = 8;

  public static final int DELAY_BETWEEN_REST_ATTEMPTS = 5000;


  private static Integer getSystemPropertyOrEnvVariable(String propertyName) {
    String jvmVariable = System.getProperty(propertyName);
    if (jvmVariable != null) {
      return !jvmVariable.isEmpty() ? Integer.parseInt(jvmVariable) : null;
    } else {
      return Optional.ofNullable(System.getenv(propertyName))
                     .map(Integer::parseInt)
                     .orElse(null);
    }
  }

  public static boolean areRetryParamsOverridden() {
    return OVERRIDE_ATTEMPT_COUNT != null || OVERRIDE_DELAY_BETWEEN_ATTEMPTS != null;
  }

  public static <V, E extends Exception> V executeOnRest(String errorMessage, GenericCallable<V, E> callable) throws E {
    return execute(errorMessage, DEFAULT_REST_ATTEMPTS, DELAY_BETWEEN_REST_ATTEMPTS, callable);
  }

  @SuppressWarnings("unchecked")
  //Suppress for throw (E), This cast does not matter on runtime level. But it is possible that it could be exception
  // of type E or RuntimeException, cause of callable type. Both are expected to be thrown by this method.
  public static <V, E extends Throwable> V execute(String errorMessage, int maxAttempts,
      int sleepTimeBetweenRetriesMs,
      GenericCallable<V, E> callable) throws E {
    maxAttempts = Optional.ofNullable(OVERRIDE_ATTEMPT_COUNT).orElse(maxAttempts);
    sleepTimeBetweenRetriesMs = Optional.ofNullable(OVERRIDE_DELAY_BETWEEN_ATTEMPTS).orElse(sleepTimeBetweenRetriesMs);
    while (true) {
      try {
        return callable.call();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RetryInterruptedException(e);
      } catch (Exception e) {
        if (--maxAttempts > 0) {
          LOGGER.warn("{} - {} Retries Left {} ", errorMessage, e.getMessage(), maxAttempts, e);
          waitForSpecificTime(sleepTimeBetweenRetriesMs);
        } else {
          LOGGER.error(errorMessage);
          throw (E) e;
        }
      }
    }
  }

  private static void waitForSpecificTime(int milliSecond) {
    try {
      Thread.sleep(milliSecond);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RetryInterruptedException(e);
    }
  }

  @SuppressWarnings("unchecked") //For "(T) enhancer.create()" - We know that method always returns valid type.
  public static <T> T createRetryProxy(T target) {
    try (
        Unloaded<? extends T> proxyType = (Unloaded<? extends T>) new ByteBuddy()
            .subclass(target.getClass())
            .method(ElementMatchers.isPublic())
            .intercept(InvocationHandlerAdapter.of((proxy, method, args) -> retryFromAnnotation(target, method, args)))
            .make()
    ) {
      return proxyType
          .load(target.getClass().getClassLoader())
          .getLoaded()
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RetryableProxyCreateException(e);
    }
  }

  @SuppressWarnings("java:S3011") //for execution: method.setAccessible(true)
  private static <T> Object retryFromAnnotation(T target, Method method, Object[] args) throws Throwable {
    Retryable retryAnnotation = getRetryAnnotation(method);
    if (retryAnnotation != null) {
      return execute(createMessage(method, retryAnnotation, args), retryAnnotation.maxAttempts(), retryAnnotation.delay(),
          () -> invokeWithThrowingOriginalException(target, method, args)
      );
    } else {
      return executeWithoutRetry(target, method, args);
    }
  }

  private static Retryable getRetryAnnotation(Method method) {
    return Optional.ofNullable(method.getAnnotation(Retryable.class))
                   .orElse(method.getDeclaringClass().getAnnotation(Retryable.class));
  }

  private static <T> Object executeWithoutRetry(T target, Method method, Object[] args) throws Throwable {
    return invokeWithThrowingOriginalException(target, method, args);
  }


  @SuppressWarnings("java:S112")
  //Suppress for "throws Throwable" - we need to support every Throwable here, because it is general solution used
  // in generated proxies, which could contain methods with different exception types.
  //The proxies are constructed so, that only exceptions declared in given method could be thrown by that method.
  private static <T> Object invokeWithThrowingOriginalException(T target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (ReflectiveOperationException e) {
      throw Optional.ofNullable(e.getCause()).orElse(e);
    }
  }

  public static String createMessage(Method method, Retryable annotation, Object[] args) {
    String result;
    if (!annotation.errorMessage().isEmpty()) {
      result = annotation.errorMessage();
    } else {
      result = String.format("Error while invoking method '%s' with args: %s", method, Arrays.toString(args));
    }
    return result;
  }

  public interface GenericCallable<V, E extends Throwable> {

    V call() throws E, InterruptedException;
  }
}
