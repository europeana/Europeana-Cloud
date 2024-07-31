package eu.europeana.cloud.service.web.common;

import static eu.europeana.cloud.common.log.AttributePassingUtils.RECORD_ID_CONTEXT_ATTR;
import static eu.europeana.cloud.common.log.AttributePassingUtils.TASK_ID_CONTEXT_ATTR;

import org.slf4j.MDC;
import org.springframework.core.task.TaskDecorator;

/**
 * Copy logging context attributes into async execution.
 */
public class LoggingContextCopingTaskDecorator implements TaskDecorator {

  @Override
  public Runnable decorate(Runnable runnable) {
    String taskId = MDC.get(TASK_ID_CONTEXT_ATTR);
    String recordId = MDC.get(RECORD_ID_CONTEXT_ATTR);
    return () -> {
      try {
        MDC.put(TASK_ID_CONTEXT_ATTR, taskId);
        MDC.put(RECORD_ID_CONTEXT_ATTR, recordId);
        runnable.run();
      } finally {
        MDC.remove(TASK_ID_CONTEXT_ATTR);
        MDC.remove(RECORD_ID_CONTEXT_ATTR);
      }
    };

  }
}
