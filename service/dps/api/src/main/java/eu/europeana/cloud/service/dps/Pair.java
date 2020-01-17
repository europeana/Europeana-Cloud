package eu.europeana.cloud.service.dps;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Pair<T1, T2> {
    private T1 object1;
    private T2 object2;

    public Pair() {
        this(null, null);
    }

    public Pair(T1 object1, T2 object2) {
        this.object1 = object1;
        this.object2 = object2;
    }
}
