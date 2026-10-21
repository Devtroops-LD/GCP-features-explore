package com.devtroops.dataflow;

import static org.junit.Assert.assertEquals;

import com.google.firestore.v1.Value;
import com.google.protobuf.NullValue;
import org.junit.Test;

public class FirestoreValuesTest {

  @Test
  public void integersBecomeIntegerValues() {
    assertEquals(42L, FirestoreValues.fromCsv("42").getIntegerValue());
    assertEquals(-7L, FirestoreValues.fromCsv(" -7 ").getIntegerValue());
  }

  @Test
  public void decimalsBecomeDoubleValues() {
    assertEquals(3.5, FirestoreValues.fromCsv("3.5").getDoubleValue(), 0.0);
    assertEquals(1200.0, FirestoreValues.fromCsv("1.2e3").getDoubleValue(), 0.0);
  }

  @Test
  public void blanksBecomeNull() {
    assertEquals(NullValue.NULL_VALUE, FirestoreValues.fromCsv("").getNullValue());
    assertEquals(NullValue.NULL_VALUE, FirestoreValues.fromCsv("   ").getNullValue());
  }

  @Test
  public void otherTextStaysString() {
    for (String text : new String[] {"E001", "2021-03-15", "NaN", "Infinity", "1d"}) {
      Value value = FirestoreValues.fromCsv(text);
      assertEquals(text, Value.ValueTypeCase.STRING_VALUE, value.getValueTypeCase());
      assertEquals(text, value.getStringValue());
    }
  }

  @Test
  public void documentIdsCannotContainSlashes() {
    assertEquals("a_b", FirestoreValues.toDocumentId(" a/b "));
  }
}
