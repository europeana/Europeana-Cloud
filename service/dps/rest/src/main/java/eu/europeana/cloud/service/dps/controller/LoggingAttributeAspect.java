package eu.europeana.cloud.service.dps.controller;

import static eu.europeana.cloud.common.log.AttributePassing.TASK_ID_CONTEXT_ATTR;

import eu.europeana.cloud.service.dps.DpsTask;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

@Aspect
public class LoggingAttributeAspect {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingAttributeAspect.class);

  @Before("execution(* *(..,@eu.europeana.cloud.service.dps.controller.AddTaskIdToLoggingContext (*),..))")
  public void beforeTaskId(JoinPoint joint) {
    String taskId = getTaskId(joint);
    MDC.put(TASK_ID_CONTEXT_ATTR, taskId);
    LOGGER.error("Aspect taskid: {}", taskId);
  }

  @After("execution(* *(..,@eu.europeana.cloud.service.dps.controller.AddTaskIdToLoggingContext (*),..))")
  public void afterTaskId() {
    MDC.remove(TASK_ID_CONTEXT_ATTR);
    LOGGER.error("After aspect taskid");
  }

  private String getTaskId(JoinPoint joint) {
    int argumentIndex = findArgumentIndex(joint);
    return extractIdFromArgument(joint.getArgs()[argumentIndex]);
  }

  private String extractIdFromArgument(Object arg) {
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
    throw new RuntimeException("Could not found argument with the annotation!");
  }

  private boolean argumentHasTheAnnotation(Annotation[] argumentAnnotations) {
    return Arrays.stream(argumentAnnotations)
                 .anyMatch(a -> a.annotationType().equals(AddTaskIdToLoggingContext.class));
  }


}
