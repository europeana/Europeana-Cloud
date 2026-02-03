package eu.europeana.cloud.service.dps.storm.utils;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MediaThrottlingFractionEvaluatorTest {

    @Test
    void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization1() {
        assertEquals(1, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(1));
    }

    @Test
    void shouldProperlyEvalForResourceProcessingWithMaxParallelization1() {
        assertEquals(1, MediaThrottlingFractionEvaluator.evalForResourceProcessing(1));
    }

    @Test
    void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization2() {
        assertEquals(1, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(2));
    }

    @Test
    void shouldProperlyEvalForResourceProcessingWithMaxParallelization2() {
        assertEquals(1, MediaThrottlingFractionEvaluator.evalForResourceProcessing(2));
    }

  @Test
  void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization3() {
      assertEquals(1, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(3));
  }

    @Test
    void shouldProperlyEvalForResourceProcessingWithMaxParallelization3() {
        assertEquals(2, MediaThrottlingFractionEvaluator.evalForResourceProcessing(3));
    }

    @Test
    void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization4() {
        assertEquals(2, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(4));
    }

    @Test
    void shouldProperlyEvalForResourceProcessingWithMaxParallelization4() {
        assertEquals(2, MediaThrottlingFractionEvaluator.evalForResourceProcessing(4));
    }

    @Test
    void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization5() {
        assertEquals(2, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(5));
    }

    @Test
    void shouldProperlyEvalForResourceProcessingWithMaxParallelization5() {
        assertEquals(3, MediaThrottlingFractionEvaluator.evalForResourceProcessing(5));
    }

    @Test
    void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization16() {
        assertEquals(7, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(16));
    }

    @Test
    void shouldProperlyEvalForResourceProcessingWithMaxParallelization16() {
        assertEquals(9, MediaThrottlingFractionEvaluator.evalForResourceProcessing(16));
    }
}