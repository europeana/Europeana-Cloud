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

    public void increaseExtremesByOne(boolean firstLine) {
        if(firstLine) {
            min = max = 1;
        } else {
            min++;
            max++;
        }
    }

    public void checkExtremes(boolean firstLine, int opened) {
        if(! (firstLine && opened == 0) ) {
            min = Math.min(min, opened);
        }
        max = Math.max(max, opened);
    }
}
