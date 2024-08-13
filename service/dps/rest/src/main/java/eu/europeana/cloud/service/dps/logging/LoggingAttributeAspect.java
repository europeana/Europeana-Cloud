package eu.europeana.cloud.service.dps.logging;

import static eu.europeana.cloud.common.log.AttributePassingUtils.TASK_ID_CONTEXT_ATTR;

import eu.europeana.cloud.service.dps.DpsTask;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.MDC;

/**
 * Aspectual implementation of log context attribute passing based on custom annotation. It fills log
 * context attributes for the method execution, if one of the method parameters is marked with
 * the custom annotation, the attribute value is gathered from this parameter.
 * Now it is used for eu.europeana.cloud.common.log.AttributePassingUtils#TASK_ID_CONTEXT_ATTR
 * which is set if method parameter containing task_id or whole DpsTask is marked with
 * the annotation: AddTaskIdToLoggingContext.
 */
@Aspect
public class LoggingAttributeAspect {

  /**
   * Adds eu.europeana.cloud.common.log.AttributePassingUtils#TASK_ID_CONTEXT_ATTR to the logging context
   * before execution of methods annotated with: AddTaskIdToLoggingContext.
   *
   * @param joint - aspect joint point object
   */
  @Before("execution(* *(..,@eu.europeana.cloud.service.dps.logging.AddTaskIdToLoggingContext (*),..))")
  public void beforeTaskId(JoinPoint joint) {
    String taskId = getTaskId(joint);
    MDC.put(TASK_ID_CONTEXT_ATTR, taskId);
  }

  /**
   * Removes eu.europeana.cloud.common.log.AttributePassingUtils#TASK_ID_CONTEXT_ATTR from the logging
   * context after execution of method with parameter annotated with: AddTaskIdToLoggingContext.
   */
  @After("execution(* *(..,@eu.europeana.cloud.service.dps.logging.AddTaskIdToLoggingContext (*),..))")
  public void afterTaskId() {
    MDC.remove(TASK_ID_CONTEXT_ATTR);
  }

  private String getTaskId(JoinPoint joint) {
    int argumentIndex = findArgumentIndex(joint);
    return extractTaskIdFromArgument(joint.getArgs()[argumentIndex]);
  }

  private String extractTaskIdFromArgument(Object arg) {
    if (arg instanceof DpsTask dpsTask) {
      return String.valueOf(dpsTask.getTaskId());
    } else {
      return String.valueOf(arg);
    }
  }

  private int findArgumentIndex(JoinPoint joint) {
    MethodSignature methodSignature = (MethodSignature) joint.getSignature();
    Annotation[][] argumentsAnnotations = methodSignature.getMethod().getParameterAnnotations();
    for (int i = 0; i < argumentsAnnotations.length; i++) {
      if (argumentHasTheAnnotation(argumentsAnnotations[i])) {
        return i;
      }
    }
    throw new IllegalArgumentException("The method in the joint does not contain argument annotated with: "
        + AddTaskIdToLoggingContext.class);
  }

  private boolean argumentHasTheAnnotation(Annotation[] argumentAnnotations) {
    return Arrays.stream(argumentAnnotations)
                 .anyMatch(a -> a.annotationType().equals(AddTaskIdToLoggingContext.class));
  }


}
