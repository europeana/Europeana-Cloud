package eu.europeana.cloud.service.dps.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CleanCronExpressionEvaluatorTest {

  @Test
  public void shouldEvaluateCleaningCronForDpsApp0() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "dps-app-0");

    assertEquals("0 0 * * * *", evaluator.getCron());
  }

  @Test
  public void shouldEvaluateCleaningCronForDpsApp1() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "dps-app-1");

    assertEquals("0 20 * * * *", evaluator.getCron());
  }

  @Test
  public void shouldEvaluateCleaningCronForDpsApp2() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "dps-app-2");

    assertEquals("0 40 * * * *", evaluator.getCron());
  }

  @Test
  public void shouldEvaluateCleaningCronForLocalApp() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "local-app");

    assertEquals("0 0 * * * *", evaluator.getCron());
  }


  @Test(expected = Exception.class)
  public void shouldThrowExceptionWhenCurrentAppNumberIsGreaterThanMaxNodeCount() {
    new CleanCronExpressionEvaluator(3, "dps-app-3");
  }

  @Test
  public void shouldEvaluateCleaningCronWhenIncreasedMaxNodeCount() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(7, "dps-app-6");

    assertEquals("0 51 * * * *", evaluator.getCron());
  }

  @Test
  public void shouldEvaluateCleaningCronIfStatefulSetNameContainsNumber() {
    CleanCronExpressionEvaluator evaluator = new CleanCronExpressionEvaluator(3, "dps-app-2-1");

    assertEquals("0 20 * * * *", evaluator.getCron());
  }

}
