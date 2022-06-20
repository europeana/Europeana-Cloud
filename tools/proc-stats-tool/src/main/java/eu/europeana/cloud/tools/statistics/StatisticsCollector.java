package eu.europeana.cloud.tools.statistics;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class StatisticsCollector implements Collector<LogLine, StatisticsAccumulator, Map<Long, Statistic>> {

    @Override
    public Supplier<StatisticsAccumulator> supplier() {
        return StatisticsAccumulator::new;
    }

    @Override
    public BiConsumer<StatisticsAccumulator, LogLine> accumulator() {
        return (StatisticsAccumulator::process);
    }

    @Override
    public BinaryOperator<StatisticsAccumulator> combiner() {
        return null;
    }

    @Override
    public Function<StatisticsAccumulator, Map<Long, Statistic>> finisher() {
        return (StatisticsAccumulator::toStatistics);
    }

    @Override
    public Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.UNORDERED);
    }
}
