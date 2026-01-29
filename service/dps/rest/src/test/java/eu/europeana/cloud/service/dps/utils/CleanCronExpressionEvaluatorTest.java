package eu.europeana.cloud.service.dps.utils;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CleanCronExpressionEvaluatorTest {

  @Test
  void shouldEvaluateCleaningCronForDpsApp0() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "dps-app-0");

    assertEquals("0 0 * * * *", evaluator.getCron());
  }

  @Test
  void shouldEvaluateCleaningCronForDpsApp1() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "dps-app-1");

    assertEquals("0 20 * * * *", evaluator.getCron());
  }

  @Test
  void shouldEvaluateCleaningCronForDpsApp2() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "dps-app-2");

    assertEquals("0 40 * * * *", evaluator.getCron());
  }

  @Test
  void shouldEvaluateCleaningCronForLocalApp() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "local-app");

    assertEquals("0 0 * * * *", evaluator.getCron());
  }


  @Test
  void shouldThrowExceptionWhenCurrentAppNumberIsGreaterThanMaxNodeCount() {
    assertThrows(Exception.class, () -> new CleanCronExpressionEvaluator(3, "dps-app-3"));
  }

  @Test
  void shouldEvaluateCleaningCronWhenIncreasedMaxNodeCount() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(7, "dps-app-6");

    assertEquals("0 51 * * * *", evaluator.getCron());
  }

  @Test
  void shouldEvaluateCleaningCronIfStatefulSetNameContainsNumber() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "dps-app-2-1");

    assertEquals("0 20 * * * *", evaluator.getCron());
  }

}
