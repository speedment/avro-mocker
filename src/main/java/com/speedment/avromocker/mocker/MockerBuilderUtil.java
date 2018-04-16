package com.speedment.avromocker.mocker;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.time.temporal.ChronoField.*;
import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

/**
 * @author Emil Forslund
 * @since  1.0.0
 */
public final class MockerBuilderUtil {

    private final static Pattern COMMA = Pattern.compile(",\\s*");

    private final static Pattern ENUM_PATTERN = Pattern.compile(
        "^(rand|incr)?(?:\\s*in\\s*([^,]+(?:,\\s*[^,]+)*))?$");

    private final static Pattern INTEGER_PATTERN = Pattern.compile(
        "^(rand|incr|date)?(?:\\s*from\\s*-?(\\d+))?(?:\\s*to\\s*-?(\\d+))?(?:\\s*scale\\s*(\\d+))?(?:\\s*in\\s*(-?\\d+(?:,\\s*-?\\d+)*))?$");

    private final static Pattern DECIMAL_PATTERN = Pattern.compile(
        "^(rand|gauss)?(?:\\s*from\\s*(-?\\d+(?:\\.\\d*)?))?(?:\\s*to\\s*(-?\\d+(?:\\.\\d*)?))?(?:\\s*prec\\s*(\\d+))?$");

    private final static Pattern STRING_PATTERN = Pattern.compile(
        "^(rand|dict)?(?:\\s*from\\s*(\\d+))?(?:\\s*to\\s*(\\d+))?(?:\\s*in\\s*([^,]+(?:,\\s*[^,]+)*))?$");

    private final static String CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZáéíóúàèìòù0123456789_&%";

    private final static class EnumPattern {
        final static int
            STRATEGY_TYPE = 1,
            SYMBOLS       = 2;
    }

    private final static class IntegerPattern {
        final static int
            STRATEGY_TYPE = 1,
            LOWER_BOUND   = 2,
            UPPER_BOUND   = 3,
            SCALE         = 4,
            SYMBOLS       = 5;
    }

    private final static class DecimalPattern {
        final static int
            STRATEGY_TYPE = 1,
            LOWER_BOUND   = 2,
            UPPER_BOUND   = 3,
            PRECISION     = 4;
    }

    private final static class StringPattern {
        final static int
            STRATEGY_TYPE = 1,
            LOWER_BOUND   = 2,
            UPPER_BOUND   = 3,
            SYMBOLS       = 4;
    }

    public static Function<Random, Object> parseField(Scanner scn, String key, Schema fieldSchema) {
        switch (fieldSchema.getType()) {
            case INT:
            case LONG:
                return parseIntegerField(scn, key, fieldSchema);
            case FLOAT:
            case DOUBLE:
                return parseDecimalField(scn, key, fieldSchema);
            case STRING:
                return parseStringField(scn, key, fieldSchema);
            case ENUM:
                return parseEnumField(scn, key, fieldSchema);
            case UNION:
                return parseUnionField(scn, key, fieldSchema);
            default:
                throw new IllegalArgumentException(format(
                    "The avro type '%s' is currently not supported.",
                    fieldSchema.getType().getName()
                ));
        }
    }

    static Function<Random, Object> parseEnumField(Scanner scn, String name, Schema fieldSchema) {
        final String typeName = fieldSchema.getType().getName();

        while (true) {
            System.out.format("Strategy for %s '%s':", typeName, name);
            final String line = scn.nextLine().trim();
            if ("help".equals(line)) {
                System.out.format(
                    "Enter a strategy to use when generating symbols for %s '%s'.%n" +
                        "  Options: %n" +
                        "    rand     : symbols are randomly distributed.%n" +
                        "    incr     : increments by one for each record.%n" +
                        "    in       : subset of symbols to select from.%n" +
                        "  Example: [incr|rand] (in <enum, enum...>)%n", typeName, name);
            } else {
                final Matcher matcher = ENUM_PATTERN.matcher(line);
                if (matcher.find()) {
                    final String strategy     = ofNullable(matcher.group(EnumPattern.STRATEGY_TYPE)).orElse("");
                    final String symbolsInput = ofNullable(matcher.group(EnumPattern.SYMBOLS)).orElse("");
                    final String[] symbols    = fieldSchema.getEnumSymbols().toArray(new String[0]);

                    // If symbols is not specified, use bounds
                    if ("".equals(symbolsInput)) {
                        switch (strategy) {
                            case "" : case "rand" : {
                                return r -> new GenericData.EnumSymbol(fieldSchema, symbols[r.nextInt(symbols.length)]);
                            }
                            case "incr" : {
                                final AtomicInteger incr = new AtomicInteger(0);
                                return r -> new GenericData.EnumSymbol(fieldSchema, symbols[incr.getAndUpdate(i -> ++i == symbols.length ? 0 : i)]);
                            }
                            default: {
                                System.err.println("Could not parse input. Enter 'help' for more info.");
                            }
                        }

                    // Parse symbols
                    } else {

                        final String[] subset;
                        try {
                            subset = COMMA.split(symbolsInput);
                        } catch (final NumberFormatException ex) {
                            System.err.println("Could not parse parameter 'in'. Enter 'help' for more info.");
                            continue;
                        }

                        if (!Stream.of(subset).allMatch(s -> Stream.of(symbols).anyMatch(s::equals))) {
                            final Set<String> wrong = new HashSet<>(asList(subset));
                            wrong.removeAll(asList(symbols));
                            System.err.format("Parameter 'in' contains invalid symbols (%s). Enter 'help' for more info.%n",
                                wrong.stream().collect(joining("', '", "'", "'")));
                            continue;
                        }

                        if (subset.length == 0) {
                            System.err.println("Parameter 'in' requires a list of numbers. Enter 'help' for more info.");
                            continue;
                        }

                        switch (strategy) {
                            case "" : case "rand" : {
                                return r -> new GenericData.EnumSymbol(fieldSchema, subset[r.nextInt(subset.length)]);
                            }
                            case "incr" : {
                                final AtomicInteger incr = new AtomicInteger(0);
                                return r -> new GenericData.EnumSymbol(fieldSchema, subset[incr.getAndUpdate(i -> ++i == subset.length ? 0 : i)]);
                            }
                            default: {
                                System.err.println("Could not parse input. Enter 'help' for more info.");
                            }
                        }
                    }
                }
            }
        }
    }

    static Function<Random, Object> parseIntegerField(Scanner scn, String name, Schema fieldSchema) {
        final String typeName = fieldSchema.getType().getName();

        while (true) {
            System.out.format("Strategy for %s '%s':", typeName, name);
            final String line = scn.nextLine().trim();
            if ("help".equals(line)) {
                System.out.format(
                    "Enter a strategy to use when generating integers for %s '%s'.%n" +
                    "  Options: %n" +
                    "    rand     : integers are randomly distributed.%n" +
                    "    incr     : increments by one for each record.%n" +
                    "    date     : increments by one for each record.%n" +
                    "    from     : the lower bound (inclusive) in span.%n" +
                    "    to       : the upper bound (exclusive) in span.%n" +
                    "    scale    : factor to multiply each value in span with.%n" +
                    "    in       : set of integers to select from.%n" +
                    "  Example: [incr|rand|date] (from <integer>) (to <integer>) (scale <integer>) (in <integer, integer...>)%n", typeName, name);
            } else {
                final Matcher matcher = INTEGER_PATTERN.matcher(line);
                if (matcher.find()) {
                    final String strategy     = ofNullable(matcher.group(IntegerPattern.STRATEGY_TYPE)).orElse("");
                    final String symbolsInput = ofNullable(matcher.group(IntegerPattern.SYMBOLS)).orElse("");
                    final String lowerInput   = ofNullable(matcher.group(IntegerPattern.LOWER_BOUND)).orElse("");
                    final String upperInput   = ofNullable(matcher.group(IntegerPattern.UPPER_BOUND)).orElse("");
                    final String scaleInput   = ofNullable(matcher.group(IntegerPattern.SCALE)).orElse("");
                    final long lower, upper;
                    final int scale;

                    // If symbols is not specified, use bounds
                    if ("".equals(symbolsInput)) {

                        // Parse lower bound
                        if ("".equals(lowerInput)) {
                            lower = 0;
                        } else {
                            try { lower = Long.parseLong(lowerInput); }
                            catch (final NumberFormatException ex) {
                                System.err.println("Could not parse parameter 'from'. Enter 'help' for more info.");
                                continue;
                            }
                        }

                        // Parse upper bound
                        if ("".equals(upperInput)) {
                            switch (fieldSchema.getType()) {
                                case INT  : upper = Integer.MAX_VALUE; break;
                                case LONG : upper = Long.MAX_VALUE;    break;
                                default : throw new IllegalStateException();
                            }
                        } else {
                            try { upper = Long.parseLong(upperInput); }
                            catch (final NumberFormatException ex) {
                                System.err.println("Could not parse parameter 'to'. Enter 'help' for more info.");
                                continue;
                            }
                        }

                        // Parse scale
                        if ("".equals(scaleInput)) {
                            scale = 1;
                        } else {
                            try { scale = Integer.parseInt(scaleInput); }
                            catch (final NumberFormatException ex) {
                                System.err.println("Could not parse parameter 'scale'. Enter 'help' for more info.");
                                continue;
                            }
                        }

                        if (upper <= lower) {
                            System.err.format("Invalid input! Illegal range from '%d' to '%d'. Enter 'help' for more info.%n", lower, upper);
                            continue; // tryAgain
                        }

                        if (scale == 0) {
                            System.err.println("Invalid input! Scale can't be zero. Enter 'help' for more info.");
                            continue; // tryAgain
                        }

                        switch (strategy) {
                            case "" : case "rand" : {
                                switch (fieldSchema.getType()) {
                                    case INT  : return r -> Math.toIntExact(nextLongBetween(r, lower, upper) * scale);
                                    case LONG : return r -> nextLongBetween(r, lower, upper) * scale;
                                    default : throw new IllegalStateException();
                                }
                            }
                            case "incr" : {
                                final AtomicLong incr = new AtomicLong(lower);
                                switch (fieldSchema.getType()) {
                                    case INT  : return r -> Math.toIntExact(incr.getAndIncrement() * scale);
                                    case LONG : return r -> incr.getAndIncrement() * scale;
                                    default : throw new IllegalStateException();
                                }
                            }
                            case "date" : {
                                final DateTimeFormatter format = new DateTimeFormatterBuilder()
                                    .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                                    .appendValue(MONTH_OF_YEAR, 2)
                                    .appendValue(DAY_OF_MONTH, 2)
                                    .toFormatter();

                                final LocalDate lowerDate = LocalDate.parse(lowerInput, format);
                                final LocalDate upperDate = LocalDate.parse(upperInput, format);
                                final int daysWidth = Math.toIntExact(ChronoUnit.DAYS.between(lowerDate, upperDate));

                                if (daysWidth <= 0) {
                                    System.err.println("Date span must be at least 1 day.");
                                    continue;
                                }

                                final int randomScale = daysWidth / scale;
                                if (randomScale == 0) {
                                    switch (fieldSchema.getType()) {
                                        case INT  : return r -> Math.toIntExact(lower);
                                        case LONG : return r -> upper;
                                        default : throw new IllegalStateException();
                                    }
                                } else {
                                    switch (fieldSchema.getType()) {
                                        case INT  : return r -> Integer.parseInt(lowerDate.plusDays(Math.min(r.nextInt(randomScale) * scale, upper - 1)).format(format));
                                        case LONG : return r -> Long.parseLong(lowerDate.plusDays(Math.min(r.nextInt(randomScale) * scale, upper - 1)).format(format));
                                        default : throw new IllegalStateException();
                                    }
                                }
                            }
                            default: {
                                System.err.println("Could not parse input. Enter 'help' for more info.");
                            }
                        }

                    // Parse symbols
                    } else {

                        final long[] symbols;
                        try {
                            symbols = Stream.of(COMMA.split(symbolsInput))
                                .mapToLong(Long::parseLong)
                                .toArray();
                        } catch (final NumberFormatException ex) {
                            System.err.println("Could not parse parameter 'in'. Enter 'help' for more info.");
                            continue;
                        }

                        if (symbols.length == 0) {
                            System.err.println("Parameter 'in' requires a list of numbers. Enter 'help' for more info.");
                            continue;
                        }

                        switch (strategy) {
                            case "" : case "rand" : {
                                switch (fieldSchema.getType()) {
                                    case INT  : return r -> Math.toIntExact(symbols[r.nextInt(symbols.length)]);
                                    case LONG : return r -> symbols[r.nextInt(symbols.length)];
                                    default : throw new IllegalStateException();
                                }
                            }
                            case "incr" : {
                                final AtomicInteger incr = new AtomicInteger(0);
                                switch (fieldSchema.getType()) {
                                    case INT  : return r -> Math.toIntExact(symbols[incr.getAndUpdate(i -> ++i == symbols.length ? 0 : i)]);
                                    case LONG : return r -> symbols[incr.getAndUpdate(i -> ++i == symbols.length ? 0 : i)];
                                    default : throw new IllegalStateException();
                                }
                            }
                            case "date" : {
                                switch (fieldSchema.getType()) {
                                    case INT  : return r -> Math.toIntExact(symbols[r.nextInt(symbols.length)]);
                                    case LONG : return r -> symbols[r.nextInt(symbols.length)];
                                    default : throw new IllegalStateException();
                                }
                            }
                            default: {
                                System.err.println("Could not parse input. Enter 'help' for more info.");
                            }
                        }
                    }
                }
            }
        }
    }

    static Function<Random, Object> parseDecimalField(Scanner scn, String name, Schema fieldSchema) {
        final String typeName = fieldSchema.getType().getName();

        while (true) {
            System.out.format("Strategy for %s '%s':", typeName, name);
            final String line = scn.nextLine().trim();
            if ("help".equals(line)) {
                System.out.format(
                    "Enter a strategy to use when generating decimal numbers for %s '%s'.%n" +
                        "  Options: %n" +
                        "    rand     : decimals are randomly distributed.%n" +
                        "    gauss    : decimals use gaussian distribution.%n" +
                        "    from     : the lower bound (inclusive) in span.%n" +
                        "    to       : the upper bound (exclusive) in span.%n" +
                        "    prec     : the decimal precision.%n" +
                        "  Example: [rand|gauss] (from <decimal>) (to <decimal>) (prec <integer>)%n", typeName, name);
            } else {
                final Matcher matcher = DECIMAL_PATTERN.matcher(line);
                if (matcher.find()) {
                    final String strategy       = ofNullable(matcher.group(DecimalPattern.STRATEGY_TYPE)).orElse("");
                    final String lowerInput     = ofNullable(matcher.group(DecimalPattern.LOWER_BOUND)).orElse("");
                    final String upperInput     = ofNullable(matcher.group(DecimalPattern.UPPER_BOUND)).orElse("");
                    final String precisionInput = ofNullable(matcher.group(DecimalPattern.PRECISION)).orElse("");
                    final double lower, upper;
                    final int precision;

                    // Parse lower bound
                    if ("".equals(lowerInput)) {
                        lower = 0;
                    } else {
                        try { lower = Double.parseDouble(lowerInput); }
                        catch (final NumberFormatException ex) {
                            System.err.println("Could not parse parameter 'from'. Enter 'help' for more info.");
                            continue;
                        }
                    }

                    // Parse upper bound
                    if ("".equals(upperInput)) {
                        switch (fieldSchema.getType()) {
                            case FLOAT  : upper = Float.MAX_VALUE;  break;
                            case DOUBLE : upper = Double.MAX_VALUE; break;
                            default : throw new IllegalStateException();
                        }
                    } else {
                        try { upper = Double.parseDouble(upperInput); }
                        catch (final NumberFormatException ex) {
                            System.err.println("Could not parse parameter 'to'. Enter 'help' for more info.");
                            continue;
                        }
                    }

                    // Parse precision
                    if ("".equals(precisionInput)) {
                        precision = 2;
                    } else {
                        try { precision = Integer.parseInt(precisionInput); }
                        catch (final NumberFormatException ex) {
                            System.err.println("Could not parse parameter 'prec'. Enter 'help' for more info.");
                            continue;
                        }
                    }

                    if (upper <= lower) {
                        System.err.format("Invalid input! Illegal range from '%f' to '%f'. Enter 'help' for more info.%n", lower, upper);
                        continue; // tryAgain
                    }

                    final double power = Math.pow(10, precision);
                    final long width = (long) ((upper - lower) * power);



                    switch (strategy) {
                        case "" : case "rand" : {
                            switch (fieldSchema.getType()) {
                                case FLOAT  : return r -> (float) (r.nextDouble() * width / power + lower);
                                case DOUBLE : return r -> r.nextDouble() * width / power + lower;
                                default : throw new IllegalStateException();
                            }
                        }
                        case "gauss" : {
                            switch (fieldSchema.getType()) {
                                case FLOAT  : return r -> (float) (r.nextGaussian() * width / power + lower);
                                case DOUBLE : return r -> r.nextGaussian() * width / power + lower;
                                default : throw new IllegalStateException();
                            }
                        }
                        default: {
                            System.err.println("Could not parse input. Enter 'help' for more info.");
                        }
                    }
                }
            }
        }
    }

    static Function<Random, Object> parseStringField(Scanner scn, String name, Schema fieldSchema) {
        final String typeName = fieldSchema.getType().getName();

        while (true) {
            System.out.format("Strategy for %s '%s':", typeName, name);
            final String line = scn.nextLine().trim();
            if ("help".equals(line)) {
                System.out.format(
                    "Enter a strategy to use when generating strings for %s '%s'.%n" +
                        "  Options: %n" +
                        "    rand     : strings are randomly distributed.%n" +
                        "    incr     : selects string by rotating over the set.%n" +
                        "    from     : minimum (inclusive) length of string.%n" +
                        "    to       : maximum (exclusive) length string.%n" +
                        "    in       : set of strings to select from.%n" +
                        "  Example: [incr|rand] (from <string>) (to <string>) (in <string, string...>)%n", typeName, name);
            } else {
                final Matcher matcher = STRING_PATTERN.matcher(line);
                if (matcher.find()) {
                    final String strategy     = ofNullable(matcher.group(StringPattern.STRATEGY_TYPE)).orElse("");
                    final String symbolsInput = ofNullable(matcher.group(StringPattern.SYMBOLS)).orElse("");
                    final String lowerInput   = ofNullable(matcher.group(StringPattern.LOWER_BOUND)).orElse("");
                    final String upperInput   = ofNullable(matcher.group(StringPattern.UPPER_BOUND)).orElse("");
                    final int lower, upper;

                    // If symbols is not specified, use bounds
                    if ("".equals(symbolsInput)) {

                        // Parse lower bound
                        if ("".equals(lowerInput)) {
                            lower = 0;
                        } else {
                            try { lower = Integer.parseInt(lowerInput); }
                            catch (final NumberFormatException ex) {
                                System.err.println("Could not parse parameter 'from'. Enter 'help' for more info.");
                                continue;
                            }
                        }

                        // Parse upper bound
                        if ("".equals(upperInput)) {
                            upper = 32;
                        } else {
                            try { upper = Integer.parseInt(upperInput); }
                            catch (final NumberFormatException ex) {
                                System.err.println("Could not parse parameter 'to'. Enter 'help' for more info.");
                                continue;
                            }
                        }

                        if (upper <= lower) {
                            System.err.format("Invalid input! Illegal range from '%d' to '%d'. Enter 'help' for more info.%n", lower, upper);
                            continue; // tryAgain
                        }

                        final int width = upper - lower;
                        return r -> {
                            final StringBuilder str = new StringBuilder();
                            final int length = r.nextInt(width) + lower;
                            for (int i = 0; i < length; i++) {
                                str.append(CHARACTERS.charAt(r.nextInt(CHARACTERS.length())));
                            }
                            return str.toString();
                        };

                    // Parse symbols
                    } else {

                        final String[] symbols = COMMA.split(symbolsInput);
                        if (symbols.length == 0) {
                            System.err.println("Parameter 'in' requires a list of strings. Enter 'help' for more info.");
                            continue;
                        }

                        switch (strategy) {
                            case "" : case "rand" : {
                                return r -> symbols[r.nextInt(symbols.length)];
                            }
                            case "incr" : {
                                final AtomicInteger incr = new AtomicInteger(0);
                                return r -> symbols[incr.getAndIncrement() % symbols.length];
                            }
                            default: {
                                System.err.println("Could not parse input. Enter 'help' for more info.");
                            }
                        }
                    }
                }
            }
        }
    }

    static Function<Random, Object> parseUnionField(Scanner scn, String name, Schema fieldSchema) {
        final String typeName = fieldSchema.getType().getName();

        final boolean nullable = fieldSchema.getTypes().stream()
            .map(Schema::getType).anyMatch(Schema.Type.NULL::equals);

        final double nullProb;
        if (nullable) {
            while (true) {
                System.out.format("Enter probability (0.0 - 1.0) that %s '%s' is null: ", typeName, name);
                final String line = scn.nextLine().trim();
                final double probability;
                try {
                    probability = Double.parseDouble(line);
                } catch (final NumberFormatException ex) {
                    System.err.println("Could not parse probability. Enter a real number between 0.0 and 1.0.");
                    continue;
                }
                if (probability < 0 || probability > 1) {
                    System.err.println("Probability must be in span 0.0 and 1.0 (inclusive).");
                    continue;
                }
                nullProb = probability;
                break;
            }
        } else nullProb = 0;

        final Set<Schema> types = fieldSchema.getTypes().stream()
            .filter(s -> !Schema.Type.NULL.equals(s.getType()))
            .collect(Collectors.toSet());

        final Schema typeSelected;
        if (types.size() > 1) {
            while (true) {
                System.out.format("Select type to generate for %s '%s': ", typeName, name);
                final String line = scn.nextLine().trim();
                if ("help".equals(line)) {
                    System.out.format(
                        "Enter one of the following: [%s].%n",
                        types.stream()
                            .map(Schema::getType)
                            .map(Schema.Type::getName)
                            .collect(joining(", ")));
                } else {
                    final Optional<Schema> selected = types.stream()
                        .filter(t -> t.getType().getName().equalsIgnoreCase(line))
                        .findAny();

                    if (selected.isPresent()) {
                        typeSelected = selected.get();
                        break;
                    }
                }
            }
        } else if (types.size() == 1) {
            typeSelected = types.iterator().next();
        } else {
            throw new IllegalArgumentException(format(
                "Avro %s field '%s' does not have at least 1 non-null type.",
                typeName,
                fieldSchema.getName()
            ));
        }

        final Function<Random, Object> inner = parseField(scn, name, typeSelected);
        if (nullable && nullProb > 0) {
            return rand -> {
                if (rand.nextDouble() <= nullProb) return null;
                else return inner.apply(rand);
            };
        } else {
            return inner;
        }
    }

    private static long nextLongBetween(Random r, long lower, long upper) {
        return Math.abs(r.nextLong()) % (upper - lower) + lower;
    }

    /**
     * Should not be instantiated.
     */
    private MockerBuilderUtil() {}
}
