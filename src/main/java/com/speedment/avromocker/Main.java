package com.speedment.avromocker;

import com.speedment.avromocker.commandline.Arguments;
import com.speedment.avromocker.mocker.Mocker;
import com.speedment.avromocker.mocker.MockerBuilder;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Random;
import java.util.Scanner;

import static com.speedment.avromocker.commandline.CommandLineUtil.parseArgs;
import static com.speedment.avromocker.mocker.MockerBuilderUtil.*;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author Emil Forslund
 * @since  1.0.0
 */
public final class Main {

    public static void main(String... arguments) {
        final Arguments args = parseArgs(arguments);
        try (final Scanner scn = newScanner(args)) {
            final Random random = new Random(System.currentTimeMillis());

            final String input = args.getAsStringOrThrow("schema");
            final String output = args.getAsString("result").orElseGet(
                () -> input.endsWith(".avsc")
                    ? (input.substring(0, input.length() - 2) + "ro")
                    : (input + ".avro"));

            final File inputFile = new File(input);
            final File outputFile = new File(output);

            final boolean clearExisting;
            if (outputFile.exists()) {
                while (true) {
                    System.out.print("Clear existing data? Y/N: ");
                    switch (scn.nextLine()) {
                        case "Y":
                        case "y": {
                            clearExisting = true;
                            break;
                        }
                        case "N":
                        case "n":
                            clearExisting = false;
                            break;
                        default: {
                            System.err.println("Please answer either 'Y' for yes or 'N' for no.");
                            continue;
                        }
                    }
                    break;
                }
            } else {
                clearExisting = false;
            }

            final Schema schema;
            try {
                schema = new Schema.Parser().parse(inputFile);
            } catch (final IOException ex) {
                throw new IllegalArgumentException(format(
                    "Error reading specified Avro Schema file '%s'.",
                    outputFile
                ));
            }

            System.out.print("How many records should be generated: ");
            final int total = Integer.parseInt(scn.nextLine());

            final MockerBuilder builder = new MockerBuilder()
                .withRandom(random);

            for (final Schema.Field field : schema.getFields()) {
                final Schema fieldSchema = field.schema();
                final String key = field.name();

                switch (fieldSchema.getType()) {
                    case INT:
                    case LONG:
                        builder.withAction(key, parseIntegerField(scn, key, fieldSchema));
                        break;
                    case FLOAT:
                    case DOUBLE:
                        builder.withAction(key, parseDecimalField(scn, key, fieldSchema));
                        break;
                    case STRING:
                        builder.withAction(key, parseStringField(scn, key, fieldSchema));
                        break;
                    case ENUM:
                        builder.withAction(key, parseEnumField(scn, key, fieldSchema));
                        break;
                    default:
                        throw new IllegalArgumentException(format(
                            "The avro type '%s' is currently not supported.",
                            fieldSchema.getType().getName()
                        ));
                }
            }

            final Mocker mocker = builder.build();

            if (clearExisting) {
                System.out.println("Deleting existing data file.");
                if (!outputFile.delete()) {
                    System.err.println("Could not delete data file.");
                    System.exit(-1);
                    return;
                }
            }

            final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            try (final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {

                System.out.format("Creating avro-file '%s'%n", outputFile);
                writer.create(schema, outputFile);

                System.out.format("Generating %,d records...%n", total);
                final GenericRecord record = new GenericData.Record(schema);
                final long timeStarted = System.currentTimeMillis();
                int i;
                for (i = 0; i < total; i++) {
                    writer.append(mocker.mock(record));

                    if (i % 1_000 == 999) {
                        writer.flush();
                    }

                    if (i % (total / 100) == total / 100 - 1) {
                        final double delta = (System.currentTimeMillis() - timeStarted) / 1000d;
                        System.out.format(
                            "...%2d%% complete. %,d rows created (%.2f rows/s).%n",
                            (i / (total / 100)), i, (i / delta)
                        );
                    }
                }

                System.out.format("Done! %,d records generated.%n", i);

            } catch (final IOException ex) {
                throw new RuntimeException(format(
                    "Error writing data to avro-file '%s'.", outputFile
                ));
            }
        }
    }

    private static Scanner newScanner(Arguments args) {
        final Optional<String> settingsFile = args.getAsString("settings");
        if (settingsFile.isPresent()) {
            try {
                final Path path = Paths.get(settingsFile.get());
                if (Files.exists(path)) {
                    return new Scanner(new SequenceInputStream(
                        Files.newInputStream(path),
                        new NonCloseableInputStream(System.in)
                    ));
                } else {
                    System.err.format("Error! Specified settings file '%s' does not exist.%n", path);
                    System.exit(-1);
                    throw new IllegalStateException();
                }
            } catch (final IOException e) {
                throw new RuntimeException(
                    "Error constructing input stream to settings file.", e
                );
            }
        } else {
            return new Scanner(new NonCloseableInputStream(System.in));
        }
    }

    private final static class NonCloseableInputStream extends InputStream {

        private final InputStream wrapped;

        public NonCloseableInputStream(InputStream wrapped) {
            this.wrapped = requireNonNull(wrapped);
        }

        @Override
        public int read() throws IOException {
            return wrapped.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return wrapped.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return wrapped.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return wrapped.skip(n);
        }

        @Override
        public int available() throws IOException {
            return wrapped.available();
        }

        @Override
        public void mark(int readlimit) {
            wrapped.mark(readlimit);
        }

        @Override
        public void reset() throws IOException {
            wrapped.reset();
        }

        @Override
        public boolean markSupported() {
            return wrapped.markSupported();
        }

        @Override
        public void close() throws IOException {
            // Do nothing.
        }
    }
}
