package eu.europeana.cloud.tools.statistics;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Statistic {
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;

    private float avg;

    /**
     * Increase number of min and max by 1
     * For first line this both values are just 1
     * @param firstLine Flag if first line from file is processed
     */
    public void increaseExtremesByOne(boolean firstLine) {
        if(firstLine) {
            min = max = 1;
        } else {
            min++;
            max++;
        }
    }

    /**
     * Count new min and max value
     * @param firstLine Flag if first line from file is processed
     * @param opened Number of opened connections
     * do not count min value for first line if no opened connections (opened == 0)
     * opened is able to have value 1 for first line, if first line has "closing" characteristic
     */
    public void checkExtremes(boolean firstLine, int opened) {
        if(! (firstLine && opened == 0) ) {
            min = Math.min(min, opened);
        }
        max = Math.max(max, opened);
    }
}
