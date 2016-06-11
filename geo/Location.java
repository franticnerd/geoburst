/* *
 * This class represents a physical location with latitude and longitude.
 */
package geo;


import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.io.Serializable;
import java.util.List;

public class Location implements Serializable {

  double lng = 0;
  double lat = 0;
  static final double EARTH_RADIUS = 6378.137;


  public Location(double lng, double lat) {
    this.lng = lng;
    this.lat = lat;
  }

  public Location(List<Double> coordinates) {
    if (coordinates.size() != 2) {
      System.out.println("Warning! The size of the coordiantes is not 2!");
    }
    lng = coordinates.get(0);
    lat = coordinates.get(1);
  }

  public double getLng() {
    return this.lng;
  }


  public double getLat() {
    return this.lat;
  }

  public RealVector toRealVector() {
    return new ArrayRealVector(new Double[] {lng, lat} );
  }

  // get the Euclidean distance to another locations, in kilometer
  public double calcEuclideanDist(Location l)    {
    double latDiff = this.lat - l.getLat();
    double lngDiff = this.lng - l.getLng();
    return Math.sqrt(latDiff*latDiff + lngDiff*lngDiff);
  }


  // get the Geographical distance to another locations, in kilometer
  public double calcGeographicDist(Location l)    {
    double lng1 = this.lng, lat1 = this.lat;
    double lng2 = l.lng, lat2 = l.lat;
    double radLat1 = rad(lat1);
    double radLat2 = rad(lat2);
    double a = radLat1 - radLat2;
    double b = rad(lng1) - rad(lng2);
    double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) + 
          Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));
    return s * EARTH_RADIUS;
  }

  // convert degree to radian
  private double rad(double d)  {
    return d * Math.PI / 180.0;
  }

  @Override
  public String toString() {
    return new String("[" + lng + " , " + lat + "]");
  }

  @Override
  public int hashCode() {
    return new Double(lat).hashCode() * new Double(lng).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof Location))
      return false;
    Location loc = (Location) obj;
    if (loc.getLat() == this.getLat() && loc.getLng() == this.getLng())
      return true;
    else
      return false;
  }

}
