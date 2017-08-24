package com.speedment.avromocker.commandline;

import com.speedment.common.function.OptionalBoolean;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author Emil Forslund
 * @since  1.0.0
 */
public final class Arguments {

    private final Map<String, String> inner;

    Arguments(Map<String, String> map) {
        this.inner = requireNonNull(map);
    }

    public Optional<String> getAsString(String key) {
        return Optional.ofNullable(inner.get(key));
    }

    public OptionalLong getAsLong(String key) {
        if (inner.containsKey(key)) {
            final String value = inner.get(key);
            if (value != null) {
                return OptionalLong.of(Long.parseLong(value));
            }
        }

        return OptionalLong.empty();
    }

    public OptionalInt getAsInt(String key) {
        if (inner.containsKey(key)) {
            final String value = inner.get(key);
            if (value != null) {
                return OptionalInt.of(Integer.parseInt(value));
            }
        }

        return OptionalInt.empty();
    }

    public OptionalBoolean getAsBoolean(String key) {
        if (inner.containsKey(key)) {
            final String value = inner.get(key);
            if (value != null) {
                return OptionalBoolean.of(Boolean.parseBoolean(value));
            }
        }

        return OptionalBoolean.empty();
    }

    public String getAsStringOrThrow(String key) {
        return getAsString(key).orElseThrow(missingKey(key));
    }

    public long getAsLongOrThrow(String key) {
        return getAsLong(key).orElseThrow(missingKey(key));
    }

    public int getAsIntOrThrow(String key) {
        return getAsInt(key).orElseThrow(missingKey(key));
    }

    public boolean getAsBooleanOrThrow(String key) {
        return getAsBoolean(key).orElseThrow(missingKey(key));
    }

    private Supplier<IllegalArgumentException> missingKey(String key) {
        return () -> new IllegalArgumentException(format(
            "Missing required argument '%s'.", key
        ));
    }
}