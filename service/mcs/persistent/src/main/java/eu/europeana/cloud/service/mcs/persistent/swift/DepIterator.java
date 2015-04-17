package eu.europeana.cloud.service.mcs.persistent.swift;

import java.util.Iterator;

public class DepIterator<T> implements Iterator<T> {

    private Iterator<T> internalIterator;
    private double switchProbability;
    private T prevoutsValue;

    public DepIterator(Iterator<T> internalIterator, double switchProbability) {
	this.internalIterator = internalIterator;
	this.switchProbability = switchProbability;
    }

    @Override
    public boolean hasNext() {
	return internalIterator.hasNext();
    }

    @Override
    public T next() {
	if (Math.random() < switchProbability && prevoutsValue != null) {
	    return prevoutsValue;
	} else {
	    return internalIterator.next();
	}

    }

    @Override
    public void remove() {
	internalIterator.remove();
    }

}
