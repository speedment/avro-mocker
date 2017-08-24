package com.speedment.avromocker.mocker;

import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * @author Emil Forslund
 * @since  1.0.0
 */
public final class Mocker {

    private final List<Consumer<GenericRecord>> actions;

    Mocker(final List<Consumer<GenericRecord>> actions) {

        this.actions  = requireNonNull(actions);
    }

    public GenericRecord mock(GenericRecord record) {
        for (final Consumer<GenericRecord> action : actions) {
            action.accept(record);
        }
        return record;
    }
}
