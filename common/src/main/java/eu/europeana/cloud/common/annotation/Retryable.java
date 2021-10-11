package eu.europeana.cloud.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface Retryable {

    int DEFAULT_DELAY_BETWEEN_ATTEMPTS = 10 * 1000;

    /**
     * Maximum number of attempts while retry mechanism works
     * Default value is set to 3
     * @return Number of attempt
     */
    int maxAttempts() default 3;

    /**
     * Number of milliseconds between fails attempt and next try
     * Default value is set to 10[s]= 10*1000[ms]
     * @return Number of milliseconds
     */
    int delay() default DEFAULT_DELAY_BETWEEN_ATTEMPTS;

    /**
     *
     * @return
     */
    String errorMessage() default "";
}
