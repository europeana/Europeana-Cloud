package eu.europeana.cloud.common.log;

import jakarta.ws.rs.client.Invocation.Builder;
import org.slf4j.MDC;

/**
 * Tool class with methods usable for log attributes passing.
 */
public class AttributePassingUtils {

  public static final String TASK_ID_CONTEXT_ATTR = "task_id";
  public static final String RECORD_ID_CONTEXT_ATTR = "record_id";
  public static final String RECORD_DELETED_CONTEXT_ATTR = "deleted";

  private AttributePassingUtils() {
  }

  /**
   * Passes on task_id and record_id attributes from the log context of the current thread,
   * to the request builder, so they will be passed further in header to the remote web server
   * during request execution.
   * @param requestBuilder
   * @return
   */
  public static Builder passLogContext(Builder requestBuilder) {
    String taskId = MDC.get(TASK_ID_CONTEXT_ATTR);
    String recordId = MDC.get(RECORD_ID_CONTEXT_ATTR);
    return requestBuilder.header(TASK_ID_CONTEXT_ATTR, taskId).header(RECORD_ID_CONTEXT_ATTR, recordId);
  }

  /**
   * Executes given runnable with the task_id added in the log context.
   *
   * @param taskId
   * @param runnable
   */
  public static void runWithTaskIdLogAttr(long taskId, Runnable runnable) {
    try {
      MDC.put(TASK_ID_CONTEXT_ATTR, String.valueOf(taskId));
      runnable.run();
    } finally {
      MDC.remove(TASK_ID_CONTEXT_ATTR);
    }
  }

}
