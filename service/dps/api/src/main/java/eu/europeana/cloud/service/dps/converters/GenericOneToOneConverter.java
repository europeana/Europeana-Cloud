package eu.europeana.cloud.service.dps.converters;

public interface GenericOneToOneConverter<T, Z> {
    Z from(T t);

    T to(Z z);
}

