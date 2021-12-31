package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;

public class RetryableMethodExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryableMethodExecutor.class);

    public static final int DEFAULT_REST_ATTEMPTS = 8;

    public static final int DELAY_BETWEEN_REST_ATTEMPTS = 5000;

    public static <E extends Exception> void executeOnRest(String errorMessage, GenericRunnable<E> runnable) throws E {
        RetryableMethodExecutor.execute(errorMessage, DEFAULT_REST_ATTEMPTS, DELAY_BETWEEN_REST_ATTEMPTS, () -> {
                    runnable.run();
                    return null;
                }
        );
    }

    public static <V, E extends Exception> V executeOnRest(String errorMessage, GenericCallable<V, E> callable) throws E {
        return execute(errorMessage, DEFAULT_REST_ATTEMPTS, DELAY_BETWEEN_REST_ATTEMPTS, callable);
    }

    public static <V, E extends Throwable> V execute(String errorMessage, int maxAttempts, int sleepTimeBetweenRetriesMs, GenericCallable<V, E> callable) throws E {
        while (true) {
            try {
                return callable.call();
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
            throw new RuntimeException("Stop waiting for retry because interrupted flag set on Thread!", e);
        }
    }

    public static <T> T createRetryProxy(T target) {
        Enhancer enhancer = new Enhancer();
        enhancer.setClassLoader(target.getClass().getClassLoader());
        enhancer.setSuperclass(target.getClass());
        enhancer.setCallback(
                (MethodInterceptor) (obj, method, args, methodProxy) -> retryFromAnnotation(target, method, args)
        );

        return (T) enhancer.create();
    }

    static <T> Object retryFromAnnotation(T target, Method method, Object[] args) throws Throwable {
        Retryable retryAnnotation = method.getAnnotation(Retryable.class);
        if (retryAnnotation != null) {
            return execute(createMessage(method, retryAnnotation, args), retryAnnotation.maxAttempts(), retryAnnotation.delay(),
                    () -> invokeWithThrowingOriginalException(target, method, args)
            );
        } else {
            return invokeWithThrowingOriginalException(target, method, args);
        }
    }

    private static <T> Object invokeWithThrowingOriginalException(T target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (ReflectiveOperationException e) {
            throw e.getCause();
        }
    }

    public static String createMessage(Method method, Retryable annotation, Object[] args) {
        String result;
        if(!annotation.errorMessage().isEmpty()) {
            result = annotation.errorMessage();
        } else {
            result = String.format("Error while invoking method '%s' with args: %s", method, Arrays.toString(args));
        }
        return result;
    }

    public interface GenericRunnable<E extends Exception> {
        void run() throws E;
    }

    public interface GenericCallable<V, E extends Throwable> {
        V call() throws E;
    }
}
