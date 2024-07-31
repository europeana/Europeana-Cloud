package eu.europeana.cloud.service.dps.logging;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 *  Annotation used for setting task id attribute in the logging context for the scope of method
 *  execution (attribute: eu.europeana.cloud.common.log.AttributePassingUtils#TASK_ID_CONTEXT_ATTR).
 *  Usage: method parameter passing task_id or DpsTask must be marked with this annotation.
 *  The attribute is added to the context before method execution and removed after it.
 */
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface AddTaskIdToLoggingContext {
}
