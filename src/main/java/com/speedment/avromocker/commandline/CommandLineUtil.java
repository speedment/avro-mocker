package com.speedment.avromocker.commandline;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Emil Forslund
 * @since  1.0.0
 */
public final class CommandLineUtil {

    public static Arguments parseArgs(String... args) {
        final Map<String, String> result = new LinkedHashMap<>();

        for (int i = 0; i < args.length;) {
            final String key, value;
            if (args[i].startsWith("-") && args[i].length() > 1) {
                key = args[i++].substring(1);
            } else {
                throw new IllegalArgumentException(format(
                    "Could not parse command line argument '%s'.",
                    args[i]
                ));
            }

            value = args[i++];
            result.put(key, value);
        }

        return new Arguments(result);
    }

    private CommandLineUtil() {}
}
