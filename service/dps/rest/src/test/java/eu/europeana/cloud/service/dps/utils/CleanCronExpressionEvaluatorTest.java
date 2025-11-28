package eu.europeana.cloud.service.dps.utils;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.context.support.TestPropertySourceUtils.addInlinedPropertiesToEnvironment;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@SuppressWarnings("squid:S5976")
//Ignored advice to use parametrized test. It did not decrease code size, instead but worsen readability
// and caused that 2 tests that are specific were executed unnecessary 4 times.
public class CleanCronExpressionEvaluatorTest {


  @Test
  public void shouldEvaluateCleaningCronForDpsApp0() {
    AnnotationConfigApplicationContext ctx = prepareSpringContext("AppId=dps-app-0");

    ctx.refresh();

    assertEquals("0 0 * * * *", ctx.getBean(CleanCronExpressionEvaluator.class).getCron());
  }

  @Test
  public void shouldEvaluateCleaningCronForDpsApp1() {
    AnnotationConfigApplicationContext ctx = prepareSpringContext("AppId=dps-app-1");

    ctx.refresh();

    assertEquals("0 20 * * * *", ctx.getBean(CleanCronExpressionEvaluator.class).getCron());
  }

  @Test
  public void shouldEvaluateCleaningCronForDpsApp2() {
    AnnotationConfigApplicationContext ctx = prepareSpringContext("AppId=dps-app-2");

    ctx.refresh();

    assertEquals("0 40 * * * *", ctx.getBean(CleanCronExpressionEvaluator.class).getCron());
  }

  @Test
  public void shouldEvaluateCleaningCronForLocalApp() {
    AnnotationConfigApplicationContext ctx = prepareSpringContext("AppId=local-app");

    ctx.refresh();

    assertEquals("0 0 * * * *", ctx.getBean(CleanCronExpressionEvaluator.class).getCron());
  }


  @Test(expected = Exception.class)
  public void shouldThrowExceptionWhenCurrentAppNumberIsGreaterThanMaxNodeCount() {
    AnnotationConfigApplicationContext ctx = prepareSpringContext("AppId=dps-app-3");

    ctx.refresh();
  }

  @Test
  public void shouldEvaluateCleaningCronWhenIcreasedMaxNodeCount() {
    AnnotationConfigApplicationContext ctx = prepareSpringContext("maxNodeCount=7", "AppId=dps-app-6");

    ctx.refresh();

    assertEquals("0 51 * * * *", ctx.getBean(CleanCronExpressionEvaluator.class).getCron());
  }

  @Test
  public void shouldEvaluateCleaningCronIfStatefulSetNameContainsNumber() {
    AnnotationConfigApplicationContext ctx = prepareSpringContext("AppId=dps-app-2-1");

    ctx.refresh();

    assertEquals("0 20 * * * *", ctx.getBean(CleanCronExpressionEvaluator.class).getCron());
  }


  private static AnnotationConfigApplicationContext prepareSpringContext(String... properties) {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    addInlinedPropertiesToEnvironment(ctx, properties);
    ctx.register(CleanCronExpressionEvaluator.class);
    return ctx;
  }

}
