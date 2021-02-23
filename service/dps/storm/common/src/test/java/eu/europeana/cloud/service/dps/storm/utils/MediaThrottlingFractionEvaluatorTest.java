package eu.europeana.cloud.service.dps.storm.utils;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import org.apache.storm.utils.TupleUtils;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

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

    @Test
    public void shouldProperlyEvalForEdmObjectProcessingWithMaxParallelization240() {
        assertEquals(96, MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(240));
    }

    @Test
    public void testTuplesDistrition() {
        int MAX_PARAL = 144;
        int BOLT_NUMBER = 144;

        Random r = new Random();
        //long taskId = r.nextLong();
        long taskId = 3333363456887490427L;
        Set<Integer> set = new TreeSet<>();
        Set<Integer> setM = new TreeSet<>();
        Set<Integer> set6 = new TreeSet<>();

        Set<Integer> setR = new TreeSet<>();
        Multiset<Integer> setF = TreeMultiset.create();



        for (Integer i = 0; i < MAX_PARAL; i++) {
            //r=new Random();
            String throttlingAttribute = taskId + "_" + i;
            setF.add(TupleUtils.chooseTaskIndex(new ArrayList<>(Collections.singletonList(throttlingAttribute)), BOLT_NUMBER));
            setM.add(BucketUtils.hash(throttlingAttribute) % BOLT_NUMBER);
            set6.add((int) (BucketUtils.hash64(throttlingAttribute) % BOLT_NUMBER));
            set.add(throttlingAttribute.hashCode() % BOLT_NUMBER);
            setR.add(r.nextInt(BOLT_NUMBER));
        }

        int maxCount=setF.entrySet().stream().mapToInt(Multiset.Entry::getCount).max().orElseThrow();
        System.out.println("Same result bolt occurence, max value: " + maxCount);
        System.out.println("Size field grouping: " + setF.elementSet().size() + " " + setF.elementSet().size() * 100 / MAX_PARAL + "%");
        System.out.println("Size hashCode(): " + set.size() + " " + set.size() * 100 / MAX_PARAL + "%");
        System.out.println("Size murmurHash(): " + setM.size() + " " + setM.size() * 100 / MAX_PARAL + "%");
        System.out.println("Size murmurHash64(): " + set6.size() + " " + set6.size() * 100 / MAX_PARAL + "%");
        System.out.println("Size random: " + setR.size() + " " + setR.size() * 100 / MAX_PARAL + "%");

    }

    @Test
    public void testFieldGrouping() {
        int MAX_PARALLALELIZATION = 144;
        int BOLT_NUMBER = 144;
        int TASK_SIZE = 9398;

        Random r = new Random();
   //     long taskId = r.nextLong();
        long taskId = 3333363456887490427L;

        Multiset<Integer> hashCodeFrequencyMultiset= TreeMultiset.create();
        Multiset<String> attributeValueFrequencyMultiset= TreeMultiset.create();

        for (Integer i = 0; i < TASK_SIZE; i++) {
            String throttlingAttribute = taskId + "_" + r.nextInt(MAX_PARALLALELIZATION);
            attributeValueFrequencyMultiset.add(throttlingAttribute);
            hashCodeFrequencyMultiset.add(TupleUtils.chooseTaskIndex(new ArrayList<>(Collections.singletonList(throttlingAttribute)), BOLT_NUMBER));

        }
        int maxCount=attributeValueFrequencyMultiset.entrySet().stream().mapToInt(Multiset.Entry::getCount).max().orElseThrow();
        System.out.println("Same attribute value occurence, max value: " + maxCount+", average "+(TASK_SIZE/MAX_PARALLALELIZATION));

        int indexCount = hashCodeFrequencyMultiset.elementSet().size();
        System.out.println("Size field grouping: " +  indexCount + " " + indexCount * 100 / MAX_PARALLALELIZATION + "%");
        maxCount=hashCodeFrequencyMultiset.entrySet().stream().mapToInt(Multiset.Entry::getCount).max().orElseThrow();
        System.out.println("Same hashcode occurences max value: " + maxCount);
        int averageBoltUsage = TASK_SIZE * 100 / (maxCount * MAX_PARALLALELIZATION);
        System.out.println("Average bolt usage: " + averageBoltUsage+"%");

    }

}