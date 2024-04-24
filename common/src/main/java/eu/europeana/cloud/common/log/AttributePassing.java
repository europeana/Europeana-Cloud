package eu.europeana.cloud.common.log;

import jakarta.ws.rs.client.Invocation.Builder;
import org.slf4j.MDC;

public class AttributePassing {

  public static final String TASK_ID_CONTEXT_ATTR = "task_id";
  public static final String RECORD_ID_CONTEXT_ATTR = "record_id";
  public static final String RECORD_DELETED_CONTEXT_ATTR = "deleted";

  private AttributePassing() {
  }

  public static Builder passLogContext(Builder request) {
    String taskId = MDC.get(TASK_ID_CONTEXT_ATTR);
    String recordId = MDC.get(RECORD_ID_CONTEXT_ATTR);
    return request.header(TASK_ID_CONTEXT_ATTR, taskId).header(RECORD_ID_CONTEXT_ATTR, recordId);
  }

  public static void runWithTaskIdLogAttr(long taskId, Runnable runnable) {
    try {
      MDC.put(TASK_ID_CONTEXT_ATTR, String.valueOf(taskId));
      runnable.run();
    } finally {
      MDC.remove(TASK_ID_CONTEXT_ATTR);
    }
  }

}
