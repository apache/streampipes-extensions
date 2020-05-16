package org.apache.streampipes.processors.geo.jvm.processor.util;

import si.uom.SI;
import tec.units.ri.quantity.Quantities;
import tec.units.ri.unit.MetricPrefix;


//import com.github.jqudt.Quantity;
//import com.github.jqudt.Unit;

import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.quantity.Length;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

public class SpLengthCalculator {

  private final double EARTHRADIUS = 6378137; //meters

  private final Unit<Length> M = SI.METRE;
  private final Unit<Length> KM = MetricPrefix.KILO(M);
  private final Unit<Length> MILE = M.multiply(1609344).divide(1000);
  private final Unit<Length> FOOT = M.multiply(3048).divide(10000);

  public enum ValidLengthUnits {
    METER(1), KM(2), MILE(3), FOOT(4);

    private final int number;

    ValidLengthUnits(int number) {
      this.number = number;
    }

    public int getNumber() {
      return number;
    }
  }


  private Integer decimalPosition;
  private Quantity<Length> length;

  // ========================= constructor
  public SpLengthCalculator(int decimalPosition) {
    this.length = Quantities.getQuantity(-9999, M);
    this.decimalPosition = decimalPosition;
  }


  // ========================== getter

  public int getDecimalPosition() {
    return decimalPosition;
  }

  public Double getLengthValueRoundet() {
    return roundResult(length.getValue().doubleValue(), this.decimalPosition);
  }

  public Double getLengthValue() {
    return (length.getValue().doubleValue());
  }

  public String getLengthUnit() {
    return length.getUnit().toString();
  }

  public Unit<Length> getUnit() {
    return length.getUnit();
  }

  // ========================== setter

  private void setLength(Quantity<Length> length) {
    this.length = length;
  }

  private void setLength(double value, Unit<Length> unit) {
    this.length = Quantities.getQuantity(value, unit);
  }


  // unit Transformation
  /**
   * convert  Quantity<Length>  value to Unit<Length>
   *
   * @param value choose target Unit<Length> from enum @see ValidLengthUnits
   */
  public void convertUnit(ValidLengthUnits value) {
    switch (value.getNumber()) {
      case 1:
        setLength(this.length.to(M));
        break;
      case 2:
        setLength(this.length.to(KM));
        break;
      case 3:
        setLength(this.length.to(MILE));
        break;
      case 4:
        setLength(this.length.to(FOOT));
        break;
    }
  }

  /**
   * convert  Quantity<Length>  value to Unit<Length>
   *
   * @param value choose int value corresponding to ValidLengthUnits
   */
  public void convertUnit(int value) {
    switch (value) {
      case 1:
        setLength(this.length.to(M));
        break;
      case 2:
        setLength(this.length.to(KM));
        break;
      case 3:
        setLength(this.length.to(MILE));
        break;
      case 4:
        setLength(this.length.to(FOOT));
        break;
    }
  }


  /**
   * transform the double value to a String depending on the {@link DecimalFormat} pattern.
   * Max 10 decimal positions are defined. negative values are interpreted as 4 digits.
   *
   * @param value
   * @param decimalPositions
   * @return
   */
  protected Double roundResult(Double value, int decimalPositions) {

    //handle negative values but should not be possible but if 3 decimal Position will be used
    if (decimalPositions < 0) {
      decimalPositions = 3;
    }
    // setup the decimal format to prevent scientific format style
    DecimalFormat df = new DecimalFormat();

    // transform the decimal
    DecimalFormatSymbols dfs = new DecimalFormatSymbols();
    // uses always the . instead of , for decimal separator
    dfs.setDecimalSeparator('.');
    df.setDecimalFormatSymbols(dfs);
    df.setGroupingUsed(false);

    // if decimal position higher than 0 is chosen, decimal Pos will be set. if negative value all
    // calc decimal position will be used
    if (decimalPositions >= 0) {
      df.setMaximumFractionDigits(decimalPositions);
    }

    //writes the result into a String to parse this into the stream. Cannot be parsed as a Double Otherwise scientific style comes back
    double result = Double.parseDouble(df.format(value));

    return result;
  }


  public void calcGeodesicDistance(double lat1, double lng1, double lat2, double lng2) {
     // using haversine formula
    double dLat = Math.toRadians(lat2-lat1);
    double dLng = Math.toRadians(lng2-lng1);
    double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
        Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
            Math.sin(dLng/2) * Math.sin(dLng/2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    float dist = (float) (EARTHRADIUS * c);

    setLength(dist, M);
  }

}
