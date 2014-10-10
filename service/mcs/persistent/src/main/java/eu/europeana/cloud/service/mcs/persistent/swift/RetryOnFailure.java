package eu.europeana.cloud.service.mcs.persistent.swift;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Indicate methods {@link DynamicBlobStore} that should be retry.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface RetryOnFailure {

}
