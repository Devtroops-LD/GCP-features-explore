package com.devtroops.dataflow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dataflow pipeline: read a CSV file and upsert each row into a Firestore collection. */
public class CsvToFirestore {

  private static final Logger LOG = LoggerFactory.getLogger(CsvToFirestore.class);
  static final String DEFAULT_DATABASE = "(default)";

  /** Also inherits --firestoreProject and --firestoreDb from {@link FirestoreOptions}. */
  public interface Options extends GcpOptions, FirestoreOptions {
    @Description("CSV file with a header row, e.g. gs://bucket/input/employees.csv")
    @Validation.Required
    String getInput();

    void setInput(String value);

    @Description("Firestore collection to write documents into")
    @Validation.Required
    String getCollection();

    void setCollection(String value);

    @Description("CSV column whose value becomes the Firestore document ID")
    @Default.String("id")
    String getKeyField();

    void setKeyField(String value);
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  static void run(Options options) throws IOException {
    FileSystems.setDefaultPipelineOptions(options);

    String project =
        options.getFirestoreProject() != null ? options.getFirestoreProject() : options.getProject();
    if (project == null || project.isEmpty()) {
      throw new IllegalArgumentException("Set --project or --firestoreProject");
    }
    String database = options.getFirestoreDb() != null ? options.getFirestoreDb() : DEFAULT_DATABASE;
    List<String> columns = readHeader(options.getInput());

    LOG.info(
        "Writing {} to Firestore projects/{}/databases/{} collection '{}' keyed by '{}'",
        options.getInput(), project, database, options.getCollection(), options.getKeyField());

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("ReadCsv", TextIO.read().from(options.getInput()).withSkipHeaderLines(1))
        .apply(
            "ToFirestoreWrites",
            new CsvLinesToFirestoreWrites(
                project, database, options.getCollection(), columns, options.getKeyField()))
        .apply("WriteFirestore", FirestoreIO.v1().write().batchWrite().build());

    PipelineResult result = pipeline.run();
    // Flex Template launch sets --templateLocation; waitUntilFinish() then throws
    // UnsupportedOperationException: "The result of template creation should not be used."
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }

  static List<String> readHeader(String input) throws IOException {
    MatchResult.Metadata file = FileSystems.matchSingleFileSpec(input);
    String headerLine;
    try (BufferedReader reader =
        new BufferedReader(
            Channels.newReader(FileSystems.open(file.resourceId()), StandardCharsets.UTF_8.name()))) {
      headerLine = reader.readLine();
    }
    return parseHeader(headerLine, input);
  }

  static List<String> parseHeader(String headerLine, String source) throws IOException {
    if (headerLine == null || headerLine.trim().isEmpty()) {
      throw new IllegalArgumentException("CSV file " + source + " is empty or has no header row");
    }
    headerLine = headerLine.replace("\uFEFF", "");
    List<String> columns = new ArrayList<>();
    try (CSVParser parser = CSVParser.parse(new StringReader(headerLine), CSVFormat.DEFAULT)) {
      for (CSVRecord record : parser) {
        record.forEach(column -> columns.add(column.trim()));
      }
    }
    return columns;
  }
}
