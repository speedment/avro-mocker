package com.speedment.avromocker.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Emil Forslund
 * @since  1.0.0
 */
public final class ShuffleUtil {

    public static void shuffle(int[] array) {
        final Random rnd = ThreadLocalRandom.current();
        for (int i = array.length - 1; i > 0; i--) {
            final int index = rnd.nextInt(i + 1);
            final int a = array[index];
            array[index] = array[i];
            array[i] = a;
        }
    }

    public static void shuffle(long[] array) {
        final Random rnd = ThreadLocalRandom.current();
        for (int i = array.length - 1; i > 0; i--) {
            final int index = rnd.nextInt(i + 1);
            final long a = array[index];
            array[index] = array[i];
            array[i] = a;
        }
    }

    private ShuffleUtil() {}

}
