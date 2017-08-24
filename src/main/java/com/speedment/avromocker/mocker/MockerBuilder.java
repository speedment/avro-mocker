package com.speedment.avromocker.mocker;

import org.apache.avro.generic.GenericRecord;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

/**
 * @author Emil Forslund
 * @since  1.0.0
 */
public final class MockerBuilder {

    private Random random;
    private final Map<String, Function<Random, Object>> actions;

    public MockerBuilder() {
        this.actions = new LinkedHashMap<>();
    }

    public MockerBuilder withRandom(Random random) {
        this.random = requireNonNull(random);
        return this;
    }

    public MockerBuilder withAction(String key, Function<Random, Object> generator) {
        actions.put(key, generator);
        return this;
    }

    public Mocker build() {
        final Random random = ofNullable(this.random).orElseGet(Random::new);
        return new Mocker(
            actions.entrySet().stream()
                .map(entry -> {
                    final String key = entry.getKey();
                    final Function<Random, Object> generator = entry.getValue();
                    return (Consumer<GenericRecord>) record -> {
                        final Object value = generator.apply(random);
                        record.put(key, value);
                    };
                }).collect(toList())
        );
    }
}
