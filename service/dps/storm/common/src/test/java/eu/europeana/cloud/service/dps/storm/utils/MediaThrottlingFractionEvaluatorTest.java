package eu.europeana.cloud.service.dps.storm.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MediaThrottlingFractionEvaluatorTest {

  @Test
  public void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization1() {
    assertEquals(1, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(1));
  }

  @Test
  public void shouldProperlyEvalForResourceProcessingWithMaxParallelization1() {
    assertEquals(1, MediaThrottlingFractionEvaluator.evalForResourceProcessing(1));
  }

  @Test
  public void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization2() {
    assertEquals(1, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(2));
  }

  @Test
  public void shouldProperlyEvalForResourceProcessingWithMaxParallelization2() {
    assertEquals(1, MediaThrottlingFractionEvaluator.evalForResourceProcessing(2));
  }

  @Test
  public void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization3() {
    assertEquals(1, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(3));
  }

  @Test
  public void shouldProperlyEvalForResourceProcessingWithMaxParallelization3() {
    assertEquals(2, MediaThrottlingFractionEvaluator.evalForResourceProcessing(3));
  }

  @Test
  public void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization4() {
    assertEquals(2, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(4));
  }

  @Test
  public void shouldProperlyEvalForResourceProcessingWithMaxParallelization4() {
    assertEquals(2, MediaThrottlingFractionEvaluator.evalForResourceProcessing(4));
  }

  @Test
  public void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization5() {
    assertEquals(2, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(5));
  }

  @Test
  public void shouldProperlyEvalForResourceProcessingWithMaxParallelization5() {
    assertEquals(3, MediaThrottlingFractionEvaluator.evalForResourceProcessing(5));
  }

  @Test
  public void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization16() {
    assertEquals(7, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(16));
  }

  @Test
  public void shouldProperlyEvalForResourceProcessingWithMaxParallelization16() {
    assertEquals(9, MediaThrottlingFractionEvaluator.evalForResourceProcessing(16));
  }
}