// A java.util.random which draws from Antithesis' SDK.

package jepsen.antithesis;

import java.util.Arrays;
import java.util.List;
import java.util.random.RandomGenerator;

public class Random implements RandomGenerator {
  public static List<Boolean> booleans = Arrays.asList(true, false);

  private final com.antithesis.sdk.Random r;

  public Random() {
    super();
    this.r = new com.antithesis.sdk.Random();
  }

  public long nextLong() {
    return r.getRandom();
  }

  public double nextDouble() {
    // Adapted from https://developer.classpath.org/doc/java/util/Random-source.html nextDouble
    // We're generating doubles in the range 0..1, so we have only the 53 bits
    // of mantissa to generate
    final long mantissa = r.getRandom() >>> (64-53);
    return mantissa / (double) (1L << 53);
  }

  public boolean nextBoolean() {
    return r.randomChoice(booleans);
  }
}
