package eu.europeana.cloud.service.dps.converters;

import java.util.List;

public interface GenericOneToManyConverter<T, Z> {
    List<Z> from(T t);

    T to(Z z);

}
