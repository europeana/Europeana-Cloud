package eu.europeana.cloud.service.dps.utils;

import jakarta.annotation.PostConstruct;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Evaluates cron expression for CleanTaskDirService it runs cleaning in different time on every dps application node. It is based
 * on the applicationIdentifier which contains POD number and maxNumber of PODs which is 3 by default, but could be configured. It
 * divides one hour to run it in regular interval. So for example for 3 nodes it runs cleaning: -at full hour on POD 0 -20 minutes
 * after full hour on POD 1 -40 minutes after full hour on POD 1
 *
 */
@Service
public class CleanCronExpressionEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(CleanCronExpressionEvaluator.class);
  public static final int CLEANING_INTERVAL_ON_SINGLE_NODE_MINUTES = 60;

  @Value("${maxNodeCount:3}")
  private int maxNodeCount;

  @Value("${AppId}")
  private String applicationId;

  @Getter
  private String cron;

  /**
   * Evaluate cron expression for current node
   */
  @PostConstruct
  public void evaluateCron() {
    Matcher matcher = Pattern.compile(".*\\D(\\d+)$").matcher(applicationId);
    int minutes;
    if (matcher.matches()) {
      int applicationNo = Integer.parseInt(matcher.group(1));
      if (applicationNo >= maxNodeCount) {
        throw new IllegalArgumentException("Application id: " + applicationId
            + " has bigger or equal number: " + applicationNo + ", than maxNodeCount: " + maxNodeCount);
      }

      minutes = CLEANING_INTERVAL_ON_SINGLE_NODE_MINUTES * applicationNo / maxNodeCount;
    } else {
      minutes = 0;
    }
    cron = String.format("0 %d * * * *", minutes);
    LOGGER.info("For this application instance: {}, evaluated dedicated cron expression: {}", applicationId, cron);
  }

}
