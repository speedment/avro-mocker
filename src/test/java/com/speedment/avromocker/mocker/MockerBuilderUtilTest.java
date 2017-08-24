package com.speedment.avromocker.mocker;

import org.apache.avro.Schema;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * @author Emil Forslund
 * @since 1.0.0
 */
@DisplayName("MockerBuilderUtil")
class MockerBuilderUtilTest {

    @TestFactory
    @DisplayName("parseEnumField")
    Stream<DynamicTest> parseEnumField() {
        final Schema fieldSchema = Schema.createEnum("Number", null, null,
            asList(
                "one", "two", "three", "four", "five",
                "six", "seven", "eight", "nine", "ten"
            )
        );

        class TestCase {
            private final String command;
            private final List<String> expected;

            public TestCase(String command, List<String> expected) {
                this.command  = command;
                this.expected = expected;
            }
        }

        return Stream.of(
            new TestCase("incr", asList("one", "two", "three", "four", "five", "six")),
            new TestCase("rand", asList("one", "three", "two", "five", "four", "seven")),
            new TestCase("rand in one", asList("one", "one", "one", "one", "one", "one")),
            new TestCase("rand in one,two", asList("one", "one", "two", "one", "two", "one")),
            new TestCase("rand in one,two,five,three", asList("one", "five", "two", "one", "three", "five")),
            new TestCase("in one", asList("one", "one", "one", "one", "one", "one")),
            new TestCase("in one,two", asList("one", "one", "two", "one", "two", "one")),
            new TestCase("incr in one,two", asList("one", "two", "one", "two", "one", "two")),
            new TestCase("in one,two,five,three", asList("one", "five", "two", "one", "three", "five"))
        ).map(test -> dynamicTest(test.command, () -> {
            final Scanner scn = new Scanner(new MockInputStream(singletonList(test.command)));
            final Function<Random, Object> actualFunc = MockerBuilderUtil.parseEnumField(scn, "testEnum", fieldSchema);
            final MockRandom actualRandom = new MockRandom();

            final List<String> actual = Stream.generate(() -> actualFunc.apply(actualRandom).toString())
                .limit(6).collect(toList());

            System.out.println();
            System.out.println("Expected: " + test.expected);
            System.out.println("Actual  : " + actual);

            Assertions.assertArrayEquals(
                test.expected.toArray(new String[0]),
                actual.toArray(new String[0]));
        }));
    }

    @TestFactory
    @DisplayName("parseIntegerField")
    Stream<DynamicTest> parseIntegerField() {

        final Schema fieldSchema = Schema.create(Schema.Type.INT);

        class TestCase {
            private final String command;
            private final List<Integer> expected;

            public TestCase(String command, List<Integer> expected) {
                this.command  = command;
                this.expected = expected;
            }
        }

        return Stream.of(
            new TestCase("incr", asList(0, 1, 2, 3, 4, 5)),
            new TestCase("rand", asList(0, 2, 1, 4, 3, 6)),
            new TestCase("rand in 1", asList(1, 1, 1, 1, 1, 1)),
            new TestCase("rand in 1,2", asList(1, 1, 2, 1, 2, 1)),
            new TestCase("rand in 1,2,5,3", asList(1, 5, 2, 1, 3, 5)),
            new TestCase("rand from 1 to 3", asList(1, 1, 2, 1, 2, 1)),
            new TestCase("in 1", asList(1, 1, 1, 1, 1, 1)),
            new TestCase("in 1,2", asList(1, 1, 2, 1, 2, 1)),
            new TestCase("incr in 1,2", asList(1, 2, 1, 2, 1, 2)),
            new TestCase("in 1,2,5,3", asList(1, 5, 2, 1, 3, 5)),
            new TestCase("from 1 to 3", asList(1, 1, 2, 1, 2, 1)),
            new TestCase("from 1 to 3 scale 3", asList(3, 3, 6, 3, 6, 3)),
            new TestCase("date from 20160101 to 20170101", asList(20160101, 20160103, 20160102, 20160105, 20160104, 20160107))
        ).map(test -> dynamicTest(test.command, () -> {
            final Scanner scn = new Scanner(new MockInputStream(singletonList(test.command)));
            final Function<Random, Object> actualFunc = MockerBuilderUtil.parseIntegerField(scn, "testInt", fieldSchema);
            final MockRandom actualRandom = new MockRandom();

            final List<Integer> actual = Stream.generate(() -> (Integer) actualFunc.apply(actualRandom))
                .limit(6).collect(toList());

            System.out.println();
            System.out.println("Expected: " + test.expected);
            System.out.println("Actual  : " + actual);

            Assertions.assertArrayEquals(
                test.expected.toArray(new Integer[0]),
                actual.toArray(new Integer[0]));
        }));
    }

    @Test @Disabled
    void parseDecimalField() {
    }

    @Test @Disabled
    void parseStringField() {
    }

    private final static class MockInputStream implements Readable {

        private final Iterator<String> it;

        MockInputStream(Iterable<String> input) {
            this.it = input.iterator();
        }

        @Override
        public int read(CharBuffer cb) throws IOException {
            if (it.hasNext()) {
                final String next = it.next();
                cb.put(next);
                return next.length();
            } else return -1;
        }
    }

    private final static class MockRandom extends Random {

        private final AtomicInteger i = new AtomicInteger(0);

        @Override
        public int nextInt() {
            return next();
        }

        @Override
        public int nextInt(int bound) {
            return next() % bound;
        }

        @Override
        public long nextLong() {
            return next();
        }

        @Override
        public boolean nextBoolean() {
            return next() % 3 == 0; // False is less common than true
        }

        @Override
        public float nextFloat() {
            return next() / 5 - 3;
        }

        @Override
        public double nextDouble() {
            return nextFloat();
        }

        @Override
        public synchronized double nextGaussian() {
            return nextFloat(); // Not correct, but works for tests
        }

        @Override
        protected int next(int bits) {
            switch (bits) {
                case  7 : case 8  : return i.getAndIncrement() & 0xff;
                case 15 : case 16 : return i.getAndIncrement() & 0xffff;
                case 31 : case 32 : return i.getAndIncrement();
                default: throw new IllegalStateException(
                    format("Invalid bit-count %d.", bits)
                );
            }
        }

        /**
         * Creates the sequence: {@code 0, 2, 1, 4, 3, 6, ...}
         * @return the next number in the sequence
         */
        private int next() {
            final int val = i.getAndIncrement();
            if (val == 0) return 0;
            else return val + (val % 2 * 2 - 1);
        }
    }
}