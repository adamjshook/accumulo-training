package com.clearedgeit.accumulo.examples.util;

public class GeoUtil {

  private static final int NUM_DIGITS = 5;

  /**
   * Encode a latitude and longitude into a z-point
   * 
   * @param latitude
   *          - A string representation of the latitude
   * @param longitude
   *          - A string representation of the longitude
   * @return The encoded location (z-point)
   */
  public static String encodeLocation(String latitude, String longitude) {

    // Normalize the latitude and longitude
    double normLat = normalizeLatitude(Double.valueOf(latitude));
    double normLong = normalizeLongitude(Double.valueOf(longitude));

    // Define the number of significant digits to index (5 in this case)
    int maxDigits = (int) Math.pow(10, NUM_DIGITS);

    // Format the lat and long: remove the decimal point and zero pad
    String sLat = String.format("%08d", ((long) (normLat * maxDigits)));
    String sLong = String.format("%08d", ((long) (normLong * maxDigits)));

    StringBuilder sb = new StringBuilder();

    // Interleave the digits
    for (int i = 0; i < sLat.length(); i++) {
      sb.append(sLat.charAt(i)).append(sLong.charAt(i));
    }

    return sb.toString();
  }

  /**
   * Decode a z-point into a latitude and longitude
   * 
   * @param encodedLocation
   *          - The encoded location (z-point)
   * @return An array of doubles holding the latitude and longitude
   */
  public static double[] decodeLocation(String encodedLocation) {
    StringBuilder sLat = new StringBuilder();
    StringBuilder sLong = new StringBuilder();

    // de-interleave the digits
    for (int i = 0; i < encodedLocation.length(); i += 2) {
      sLat.append(encodedLocation.charAt(i));
      sLong.append(encodedLocation.charAt(i + 1));
    }

    // Define the number of significant digits to index (5 in this case)
    int maxDigits = (int) Math.pow(10, NUM_DIGITS);

    // Format the lat and long: remove the decimal point and zero pad
    double dLat = deNormalizeLatitude(Double.valueOf(sLat.toString())
        / maxDigits);
    double dLong = deNormalizeLongitude(Double.valueOf(sLong.toString())
        / maxDigits);

    return new double[] { dLat, dLong };
  }

  /**
   * Converts a latitude range from [-90, 90] to [0,180]
   * 
   * @param latitude
   *          - A double representation of the latitude
   * @return The normalized latitude
   */
  private static double normalizeLatitude(double latitude) {
    return latitude + 90;
  }

  /**
   * Converts a longitude range from [-180,180] to [0,360]
   * 
   * @param longitude
   *          - A double representation of the longitude
   * @return The normalized longitude
   */
  private static double normalizeLongitude(double longitude) {
    return longitude + 180;
  }

  /**
   * Converts a latitude range from [0,180] to [-90,90]
   * 
   * @param latitude
   *          - A double representation of the latitude
   * @return The denormalized latitude
   */
  private static double deNormalizeLatitude(double latitude) {
    return latitude - 90;
  }

  /**
   * Converts a longitude range from [0,360] to [-180,180]
   * 
   * @param longitude
   *          - A double representation of the longitude
   * @return the denormalized longitude
   */
  private static double deNormalizeLongitude(double longitude) {
    return longitude - 180;
  }
}
