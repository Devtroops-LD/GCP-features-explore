package com.devtroops.dataflow;

import static org.junit.Assert.assertEquals;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class CsvLinesToFirestoreWritesTest {

  private static final List<String> COLUMNS =
      Arrays.asList("id", "name", "department", "salary", "joined_on");
  private static final String PREFIX = "projects/p/databases/(default)/documents/employees/";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void convertsRowsToUpsertWritesAndSkipsBadRows() {
    PCollection<Write> writes =
        pipeline
            .apply(
                Create.of(
                    "E001,\"Sharma, Aarav\",Engineering,95000,2021-03-15",
                    "E002,Diya,Marketing",
                    ",NoKey,HR,1,2020-01-01",
                    ""))
            .apply(new CsvLinesToFirestoreWrites("p", "(default)", "employees", COLUMNS, "id"));

    Write expected =
        Write.newBuilder()
            .setUpdate(
                Document.newBuilder()
                    .setName(PREFIX + "E001")
                    .putFields("id", string("E001"))
                    .putFields("name", string("Sharma, Aarav"))
                    .putFields("department", string("Engineering"))
                    .putFields("salary", Value.newBuilder().setIntegerValue(95000).build())
                    .putFields("joined_on", string("2021-03-15")))
            .build();

    PAssert.that(writes).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsUnknownKeyField() {
    new CsvLinesToFirestoreWrites("p", "(default)", "employees", COLUMNS, "employee_id");
  }

  @Test
  public void parsesHeaderWithBomAndQuotes() throws Exception {
    assertEquals(
        Arrays.asList("id", "full name", "salary"),
        CsvToFirestore.parseHeader("\uFEFFid,\"full name\", salary", "test.csv"));
  }

  private static Value string(String text) {
    return Value.newBuilder().setStringValue(text).build();
  }
}
