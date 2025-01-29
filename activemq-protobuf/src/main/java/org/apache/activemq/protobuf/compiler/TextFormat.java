// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.
// http://code.google.com/p/protobuf/
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.activemq.protobuf.compiler;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.protobuf.UTF8Buffer;

/** 
 * Provide ascii text parsing and formatting support for proto2 instances.
 * The implementation largely follows google/protobuf/text_format.cc.
 *
 * HRC: I wish the original class was not package protected so we did not need
 * to copy this file over.  We need to request that the protobuf folks open 
 * this class up amoung a few others. 
 * 
 * @author wenboz@google.com Wenbo Zhu
 * @author kenton@google.com Kenton Varda
 */
public final class TextFormat {

  /** Convert an unsigned 32-bit integer to a string. */
  private static String unsignedToString(int value) {
    if (value >= 0) {
      return Integer.toString(value);
    } else {
      return Long.toString(((long) value) & 0x00000000FFFFFFFFL);
    }
  }

  /** Convert an unsigned 64-bit integer to a string. */
  private static String unsignedToString(long value) {
    if (value >= 0) {
      return Long.toString(value);
    } else {
      // Pull off the most-significant bit so that BigInteger doesn't think
      // the number is negative, then set it again using setBit().
      return BigInteger.valueOf(value & 0x7FFFFFFFFFFFFFFFL)
                       .setBit(63).toString();
    }
  }

  // =================================================================
  // Parsing

  /**
   * Represents a stream of tokens parsed from a {@code String}.
   *
   * <p>The Java standard library provides many classes that you might think
   * would be useful for implementing this, but aren't.  For example:
   *
   * <ul>
   * <li>{@code java.io.StreamTokenizer}:  This almost does what we want -- or,
   *   at least, something that would get us close to what we want -- except
   *   for one fatal flaw:  It automatically un-escapes strings using Java
   *   escape sequences, which do not include all the escape sequences we
   *   need to support (e.g. '\x').
   * <li>{@code java.util.Scanner}:  This seems like a great way at least to
   *   parse regular expressions out of a stream (so we wouldn't have to load
   *   the entire input into a single string before parsing).  Sadly,
   *   {@code Scanner} requires that tokens be delimited with some delimiter.
   *   Thus, although the text "foo:" should parse to two tokens ("foo" and
   *   ":"), {@code Scanner} would recognize it only as a single token.
   *   Furthermore, {@code Scanner} provides no way to inspect the contents
   *   of delimiters, making it impossible to keep track of line and column
   *   numbers.
   * </ul>
   *
   * <p>Luckily, Java's regular expression support does manage to be useful to
   * us.  (Barely:  We need {@code Matcher.usePattern()}, which is new in
   * Java 1.5.)  So, we can use that, at least.  Unfortunately, this implies
   * that we need to have the entire input in one contiguous string.
   */
  private static final class Tokenizer {
    private final CharSequence text;
    private final Matcher matcher;
    private String currentToken;

    // The character index within this.text at which the current token begins.
    private int pos = 0;

    // The line and column numbers of the current token.
    private int line = 0;
    private int column = 0;

    // The line and column numbers of the previous token (allows throwing
    // errors *after* consuming).
    private int previousLine = 0;
    private int previousColumn = 0;

    private static Pattern WHITESPACE =
      Pattern.compile("(\\s|(#.*$))+", Pattern.MULTILINE);
    private static Pattern TOKEN = Pattern.compile(
      "[a-zA-Z_][0-9a-zA-Z_+-]*|" +                 // an identifier
      "[0-9+-][0-9a-zA-Z_.+-]*|" +                  // a number
      "\"([^\"\n\\\\]|\\\\.)*(\"|\\\\?$)|" +        // a double-quoted string
      "\'([^\"\n\\\\]|\\\\.)*(\'|\\\\?$)",          // a single-quoted string
      Pattern.MULTILINE);

    private static Pattern DOUBLE_INFINITY = Pattern.compile(
      "-?inf(inity)?",
      Pattern.CASE_INSENSITIVE);
    private static Pattern FLOAT_INFINITY = Pattern.compile(
      "-?inf(inity)?f?",
      Pattern.CASE_INSENSITIVE);
    private static Pattern FLOAT_NAN = Pattern.compile(
      "nanf?",
      Pattern.CASE_INSENSITIVE);

    /** Construct a tokenizer that parses tokens from the given text. */
    public Tokenizer(CharSequence text) {
      this.text = text;
      this.matcher = WHITESPACE.matcher(text);
      skipWhitespace();
      nextToken();
    }

    /** Are we at the end of the input? */
    public boolean atEnd() {
      return currentToken.length() == 0;
    }

    /** Advance to the next token. */
    public void nextToken() {
      previousLine = line;
      previousColumn = column;

      // Advance the line counter to the current position.
      while (pos < matcher.regionStart()) {
        if (text.charAt(pos) == '\n') {
          ++line;
          column = 0;
        } else {
          ++column;
        }
        ++pos;
      }

      // Match the next token.
      if (matcher.regionStart() == matcher.regionEnd()) {
        // EOF
        currentToken = "";
      } else {
        matcher.usePattern(TOKEN);
        if (matcher.lookingAt()) {
          currentToken = matcher.group();
          matcher.region(matcher.end(), matcher.regionEnd());
        } else {
          // Take one character.
          currentToken = String.valueOf(text.charAt(pos));
          matcher.region(pos + 1, matcher.regionEnd());
        }

        skipWhitespace();
      }
    }

    /**
     * Skip over any whitespace so that the matcher region starts at the next
     * token.
     */
    private void skipWhitespace() {
      matcher.usePattern(WHITESPACE);
      if (matcher.lookingAt()) {
        matcher.region(matcher.end(), matcher.regionEnd());
      }
    }

    /**
     * If the next token exactly matches {@code token}, consume it and return
     * {@code true}.  Otherwise, return {@code false} without doing anything.
     */
    public boolean tryConsume(String token) {
      if (currentToken.equals(token)) {
        nextToken();
        return true;
      } else {
        return false;
      }
    }

    /**
     * If the next token exactly matches {@code token}, consume it.  Otherwise,
     * throw a {@link ParseException}.
     */
    public void consume(String token) throws ParseException {
      if (!tryConsume(token)) {
        throw parseException("Expected \"" + token + "\".");
      }
    }

    /**
     * Returns {@code true} if the next token is an integer, but does
     * not consume it.
     */
    public boolean lookingAtInteger() {
      if (currentToken.length() == 0) {
        return false;
      }

      char c = currentToken.charAt(0);
      return ('0' <= c && c <= '9') ||
             c == '-' || c == '+';
    }

    /**
     * If the next token is an identifier, consume it and return its value.
     * Otherwise, throw a {@link ParseException}.
     */
    public String consumeIdentifier() throws ParseException {
      for (int i = 0; i < currentToken.length(); i++) {
        char c = currentToken.charAt(i);
        if (('a' <= c && c <= 'z') ||
            ('A' <= c && c <= 'Z') ||
            ('0' <= c && c <= '9') ||
            (c == '_') || (c == '.')) {
          // OK
        } else {
          throw parseException("Expected identifier.");
        }
      }

      String result = currentToken;
      nextToken();
      return result;
    }

    /**
     * If the next token is a 32-bit signed integer, consume it and return its
     * value.  Otherwise, throw a {@link ParseException}.
     */
    public int consumeInt32() throws ParseException {
      try {
        int result = parseInt32(currentToken);
        nextToken();
        return result;
      } catch (NumberFormatException e) {
        throw integerParseException(e);
      }
    }

    /**
     * If the next token is a 32-bit unsigned integer, consume it and return its
     * value.  Otherwise, throw a {@link ParseException}.
     */
    public int consumeUInt32() throws ParseException {
      try {
        int result = parseUInt32(currentToken);
        nextToken();
        return result;
      } catch (NumberFormatException e) {
        throw integerParseException(e);
      }
    }

    /**
     * If the next token is a 64-bit signed integer, consume it and return its
     * value.  Otherwise, throw a {@link ParseException}.
     */
    public long consumeInt64() throws ParseException {
      try {
        long result = parseInt64(currentToken);
        nextToken();
        return result;
      } catch (NumberFormatException e) {
        throw integerParseException(e);
      }
    }

    /**
     * If the next token is a 64-bit unsigned integer, consume it and return its
     * value.  Otherwise, throw a {@link ParseException}.
     */
    public long consumeUInt64() throws ParseException {
      try {
        long result = parseUInt64(currentToken);
        nextToken();
        return result;
      } catch (NumberFormatException e) {
        throw integerParseException(e);
      }
    }

    /**
     * If the next token is a double, consume it and return its value.
     * Otherwise, throw a {@link ParseException}.
     */
    public double consumeDouble() throws ParseException {
      // We need to parse infinity and nan separately because
      // Double.parseDouble() does not accept "inf", "infinity", or "nan".
      if (DOUBLE_INFINITY.matcher(currentToken).matches()) {
        boolean negative = currentToken.startsWith("-");
        nextToken();
        return negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
      }
      if (currentToken.equalsIgnoreCase("nan")) {
        nextToken();
        return Double.NaN;
      }
      try {
        double result = Double.parseDouble(currentToken);
        nextToken();
        return result;
      } catch (NumberFormatException e) {
        throw floatParseException(e);
      }
    }

    /**
     * If the next token is a float, consume it and return its value.
     * Otherwise, throw a {@link ParseException}.
     */
    public float consumeFloat() throws ParseException {
      // We need to parse infinity and nan separately because
      // Float.parseFloat() does not accept "inf", "infinity", or "nan".
      if (FLOAT_INFINITY.matcher(currentToken).matches()) {
        boolean negative = currentToken.startsWith("-");
        nextToken();
        return negative ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
      }
      if (FLOAT_NAN.matcher(currentToken).matches()) {
        nextToken();
        return Float.NaN;
      }
      try {
        float result = Float.parseFloat(currentToken);
        nextToken();
        return result;
      } catch (NumberFormatException e) {
        throw floatParseException(e);
      }
    }

    /**
     * If the next token is a boolean, consume it and return its value.
     * Otherwise, throw a {@link ParseException}.
     */
    public boolean consumeBoolean() throws ParseException {
      if (currentToken.equals("true")) {
        nextToken();
        return true;
      } else if (currentToken.equals("false")) {
        nextToken();
        return false;
      } else {
        throw parseException("Expected \"true\" or \"false\".");
      }
    }

    /**
     * If the next token is a string, consume it and return its (unescaped)
     * value.  Otherwise, throw a {@link ParseException}.
     */
    public String consumeString() throws ParseException {
      return new UTF8Buffer(consumeBuffer()).toString();
    }

    /**
     * If the next token is a string, consume it, unescape it as a
     * {@link Buffer}, and return it.  Otherwise, throw a
     * {@link ParseException}.
     */
    public Buffer consumeBuffer() throws ParseException {
      char quote = currentToken.length() > 0 ? currentToken.charAt(0) : '\0';
      if (quote != '\"' && quote != '\'') {
        throw parseException("Expected string.");
      }

      if (currentToken.length() < 2 ||
          currentToken.charAt(currentToken.length() - 1) != quote) {
        throw parseException("String missing ending quote.");
      }

      try {
        String escaped = currentToken.substring(1, currentToken.length() - 1);
        Buffer result = unescapeBytes(escaped);
        nextToken();
        return result;
      } catch (InvalidEscapeSequence e) {
        throw parseException(e.getMessage());
      }
    }

    /**
     * Returns a {@link ParseException} with the current line and column
     * numbers in the description, suitable for throwing.
     */
    public ParseException parseException(String description) {
      // Note:  People generally prefer one-based line and column numbers.
      return new ParseException(
        (line + 1) + ":" + (column + 1) + ": " + description);
    }

    /**
     * Returns a {@link ParseException} with the line and column numbers of
     * the previous token in the description, suitable for throwing.
     */
    public ParseException parseExceptionPreviousToken(String description) {
      // Note:  People generally prefer one-based line and column numbers.
      return new ParseException(
        (previousLine + 1) + ":" + (previousColumn + 1) + ": " + description);
    }

    /**
     * Constructs an appropriate {@link ParseException} for the given
     * {@code NumberFormatException} when trying to parse an integer.
     */
    private ParseException integerParseException(NumberFormatException e) {
      return parseException("Couldn't parse integer: " + e.getMessage());
    }

    /**
     * Constructs an appropriate {@link ParseException} for the given
     * {@code NumberFormatException} when trying to parse a float or double.
     */
    private ParseException floatParseException(NumberFormatException e) {
      return parseException("Couldn't parse number: " + e.getMessage());
    }
  }

  /** Thrown when parsing an invalid text format message. */
  public static class ParseException extends IOException {
    public ParseException(String message) {
      super(message);
    }
  }

  private static final int BUFFER_SIZE = 4096;

  // TODO(chrisn): See if working around java.io.Reader#read(CharBuffer)
  // overhead is worthwhile
  private static StringBuilder toStringBuilder(Readable input)
      throws IOException {
    StringBuilder text = new StringBuilder();
    CharBuffer buffer = CharBuffer.allocate(BUFFER_SIZE);
    while (true) {
      int n = input.read(buffer);
      if (n == -1) {
        break;
      }
      buffer.flip();
      text.append(buffer, 0, n);
    }
    return text;
  }


  // =================================================================
  // Utility functions
  //
  // Some of these methods are package-private because Descriptors.java uses
  // them.

  /**
   * Escapes bytes in the format used in protocol buffer text format, which
   * is the same as the format used for C string literals.  All bytes
   * that are not printable 7-bit ASCII characters are escaped, as well as
   * backslash, single-quote, and double-quote characters.  Characters for
   * which no defined short-hand escape sequence is defined will be escaped
   * using 3-digit octal sequences.
   */
  static String escapeBytes(Buffer input) {
    StringBuilder builder = new StringBuilder(input.getLength());
    for (int i = 0; i < input.getLength(); i++) {
      byte b = input.byteAt(i);
      switch (b) {
        // Java does not recognize \a or \v, apparently.
        case 0x07: builder.append("\\a" ); break;
        case '\b': builder.append("\\b" ); break;
        case '\f': builder.append("\\f" ); break;
        case '\n': builder.append("\\n" ); break;
        case '\r': builder.append("\\r" ); break;
        case '\t': builder.append("\\t" ); break;
        case 0x0b: builder.append("\\v" ); break;
        case '\\': builder.append("\\\\"); break;
        case '\'': builder.append("\\\'"); break;
        case '"' : builder.append("\\\""); break;
        default:
          if (b >= 0x20) {
            builder.append((char) b);
          } else {
            builder.append('\\');
            builder.append((char) ('0' + ((b >>> 6) & 3)));
            builder.append((char) ('0' + ((b >>> 3) & 7)));
            builder.append((char) ('0' + (b & 7)));
          }
          break;
      }
    }
    return builder.toString();
  }

  /**
   * Un-escape a byte sequence as escaped using
   * {@link #escapeBytes(Buffer)}.  Two-digit hex escapes (starting with
   * "\x") are also recognized.
   */
  static Buffer unescapeBytes(CharSequence input)
      throws InvalidEscapeSequence {
    byte[] result = new byte[input.length()];
    int pos = 0;
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (c == '\\') {
        if (i + 1 < input.length()) {
          ++i;
          c = input.charAt(i);
          if (isOctal(c)) {
            // Octal escape.
            int code = digitValue(c);
            if (i + 1 < input.length() && isOctal(input.charAt(i + 1))) {
              ++i;
              code = code * 8 + digitValue(input.charAt(i));
            }
            if (i + 1 < input.length() && isOctal(input.charAt(i + 1))) {
              ++i;
              code = code * 8 + digitValue(input.charAt(i));
            }
            result[pos++] = (byte)code;
          } else {
            switch (c) {
              case 'a' : result[pos++] = 0x07; break;
              case 'b' : result[pos++] = '\b'; break;
              case 'f' : result[pos++] = '\f'; break;
              case 'n' : result[pos++] = '\n'; break;
              case 'r' : result[pos++] = '\r'; break;
              case 't' : result[pos++] = '\t'; break;
              case 'v' : result[pos++] = 0x0b; break;
              case '\\': result[pos++] = '\\'; break;
              case '\'': result[pos++] = '\''; break;
              case '"' : result[pos++] = '\"'; break;

              case 'x':
                // hex escape
                int code = 0;
                if (i + 1 < input.length() && isHex(input.charAt(i + 1))) {
                  ++i;
                  code = digitValue(input.charAt(i));
                } else {
                  throw new InvalidEscapeSequence(
                    "Invalid escape sequence: '\\x' with no digits");
                }
                if (i + 1 < input.length() && isHex(input.charAt(i + 1))) {
                  ++i;
                  code = code * 16 + digitValue(input.charAt(i));
                }
                result[pos++] = (byte)code;
                break;

              default:
                throw new InvalidEscapeSequence(
                  "Invalid escape sequence: '\\" + c + "'");
            }
          }
        } else {
          throw new InvalidEscapeSequence(
            "Invalid escape sequence: '\\' at end of string.");
        }
      } else {
        result[pos++] = (byte)c;
      }
    }

    return new Buffer(result, 0, pos);
  }

  /**
   * Thrown by {@link TextFormat#unescapeBytes} and
   * {@link TextFormat#unescapeText} when an invalid escape sequence is seen.
   */
  static class InvalidEscapeSequence extends IOException {
    public InvalidEscapeSequence(String description) {
      super(description);
    }
  }

  /**
   * Like {@link #escapeBytes(Buffer)}, but escapes a text string.
   * Non-ASCII characters are first encoded as UTF-8, then each byte is escaped
   * individually as a 3-digit octal escape.  Yes, it's weird.
   */
  static String escapeText(String input) {
    return escapeBytes(new UTF8Buffer(input));
  }

  /**
   * Un-escape a text string as escaped using {@link #escapeText(String)}.
   * Two-digit hex escapes (starting with "\x") are also recognized.
   */
  static String unescapeText(String input) throws InvalidEscapeSequence {
    return new UTF8Buffer(unescapeBytes(input)).toString();
  }

  /** Is this an octal digit? */
  private static boolean isOctal(char c) {
    return '0' <= c && c <= '7';
  }

  /** Is this a hex digit? */
  private static boolean isHex(char c) {
    return ('0' <= c && c <= '9') ||
           ('a' <= c && c <= 'f') ||
           ('A' <= c && c <= 'F');
  }

  /**
   * Interpret a character as a digit (in any base up to 36) and return the
   * numeric value.  This is like {@code Character.digit()} but we don't accept
   * non-ASCII digits.
   */
  private static int digitValue(char c) {
    if ('0' <= c && c <= '9') {
      return c - '0';
    } else if ('a' <= c && c <= 'z') {
      return c - 'a' + 10;
    } else {
      return c - 'A' + 10;
    }
  }

  /**
   * Parse a 32-bit signed integer from the text.  Unlike the Java standard
   * {@code Integer.parseInt()}, this function recognizes the prefixes "0x"
   * and "0" to signify hexidecimal and octal numbers, respectively.
   */
  static int parseInt32(String text) throws NumberFormatException {
    return (int) parseInteger(text, true, false);
  }

  /**
   * Parse a 32-bit unsigned integer from the text.  Unlike the Java standard
   * {@code Integer.parseInt()}, this function recognizes the prefixes "0x"
   * and "0" to signify hexidecimal and octal numbers, respectively.  The
   * result is coerced to a (signed) {@code int} when returned since Java has
   * no unsigned integer type.
   */
  static int parseUInt32(String text) throws NumberFormatException {
    return (int) parseInteger(text, false, false);
  }

  /**
   * Parse a 64-bit signed integer from the text.  Unlike the Java standard
   * {@code Integer.parseInt()}, this function recognizes the prefixes "0x"
   * and "0" to signify hexidecimal and octal numbers, respectively.
   */
  static long parseInt64(String text) throws NumberFormatException {
    return parseInteger(text, true, true);
  }

  /**
   * Parse a 64-bit unsigned integer from the text.  Unlike the Java standard
   * {@code Integer.parseInt()}, this function recognizes the prefixes "0x"
   * and "0" to signify hexidecimal and octal numbers, respectively.  The
   * result is coerced to a (signed) {@code long} when returned since Java has
   * no unsigned long type.
   */
  static long parseUInt64(String text) throws NumberFormatException {
    return parseInteger(text, false, true);
  }

  private static long parseInteger(String text,
                                   boolean isSigned,
                                   boolean isLong)
                                   throws NumberFormatException {
    int pos = 0;

    boolean negative = false;
    if (text.startsWith("-", pos)) {
      if (!isSigned) {
        throw new NumberFormatException("Number must be positive: " + text);
      }
      ++pos;
      negative = true;
    }

    int radix = 10;
    if (text.startsWith("0x", pos)) {
      pos += 2;
      radix = 16;
    } else if (text.startsWith("0", pos)) {
      radix = 8;
    }

    String numberText = text.substring(pos);

    long result = 0;
    if (numberText.length() < 16) {
      // Can safely assume no overflow.
      result = Long.parseLong(numberText, radix);
      if (negative) {
        result = -result;
      }

      // Check bounds.
      // No need to check for 64-bit numbers since they'd have to be 16 chars
      // or longer to overflow.
      if (!isLong) {
        if (isSigned) {
          if (result > Integer.MAX_VALUE || result < Integer.MIN_VALUE) {
            throw new NumberFormatException(
              "Number out of range for 32-bit signed integer: " + text);
          }
        } else {
          if (result >= (1L << 32) || result < 0) {
            throw new NumberFormatException(
              "Number out of range for 32-bit unsigned integer: " + text);
          }
        }
      }
    } else {
      BigInteger bigValue = new BigInteger(numberText, radix);
      if (negative) {
        bigValue = bigValue.negate();
      }

      // Check bounds.
      if (!isLong) {
        if (isSigned) {
          if (bigValue.bitLength() > 31) {
            throw new NumberFormatException(
              "Number out of range for 32-bit signed integer: " + text);
          }
        } else {
          if (bigValue.bitLength() > 32) {
            throw new NumberFormatException(
              "Number out of range for 32-bit unsigned integer: " + text);
          }
        }
      } else {
        if (isSigned) {
          if (bigValue.bitLength() > 63) {
            throw new NumberFormatException(
              "Number out of range for 64-bit signed integer: " + text);
          }
        } else {
          if (bigValue.bitLength() > 64) {
            throw new NumberFormatException(
              "Number out of range for 64-bit unsigned integer: " + text);
          }
        }
      }

      result = bigValue.longValue();
    }

    return result;
  }
}
