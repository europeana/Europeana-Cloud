package eu.europeana.cloud.service.mcs.persistent.swift;

import java.util.Iterator;

/**
 * Class iterate over objects. Returns next element if random number is grather
 * than swithProbability or return previous elemant.
 * 
 * @param <T>
 */
public class RandomIterator<T> implements Iterator<T> {

    private final Iterator<T> internalIterator;
    private final double switchProbability;
    private T prevoutsValue;


    /**
     * Class constructor.
     * 
     * @param internalIterator
     *            decorated iterator
     * @param switchProbability
     *            switch probability should be number in range (0.0 - 1.0)
     */
    public RandomIterator(final Iterator<T> internalIterator, final double switchProbability) {
        if (!(switchProbability > 0.0 && switchProbability < 1.0)) {
            throw new RuntimeException("switchProbability should be number in range (0.0 - 1.0)");
        }
        this.internalIterator = internalIterator;
        this.switchProbability = switchProbability;
    }


    @Override
    public boolean hasNext() {
        return internalIterator.hasNext();
    }


    /**
     * Returns next element if random number is grather than swithProbability or
     * return previous elemant.
     * 
     * @return the element in the iteration
     */
    public T next() {
        if (Math.random() > switchProbability || prevoutsValue == null) {
            prevoutsValue = internalIterator.next();
        }
        return prevoutsValue;

    }


    @Override
    public void remove() {
        internalIterator.remove();
    }

}
