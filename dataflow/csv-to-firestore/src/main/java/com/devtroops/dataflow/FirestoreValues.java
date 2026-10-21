package com.devtroops.dataflow;

import com.google.firestore.v1.Value;
import com.google.protobuf.NullValue;

/** Converts CSV cell text into Firestore values. */
public final class FirestoreValues {

  private FirestoreValues() {}

  /** Integers and finite decimals become numbers, blank cells become null, everything else stays a string. */
  public static Value fromCsv(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    }
    String text = raw.trim();
    try {
      return Value.newBuilder().setIntegerValue(Long.parseLong(text)).build();
    } catch (NumberFormatException ignored) {
      // not an integer
    }
    if (looksNumeric(text)) {
      try {
        double number = Double.parseDouble(text);
        if (Double.isFinite(number)) {
          return Value.newBuilder().setDoubleValue(number).build();
        }
      } catch (NumberFormatException ignored) {
        // not a decimal
      }
    }
    return Value.newBuilder().setStringValue(text).build();
  }

  /** Firestore document IDs cannot contain '/'. */
  public static String toDocumentId(String key) {
    return key.trim().replace('/', '_');
  }

  // Double.parseDouble also accepts "NaN", "Infinity" and suffixes like "1d", which should stay strings.
  private static boolean looksNumeric(String text) {
    return text.matches("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?");
  }
}
