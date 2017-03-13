package KMeansMR;

import java.io.DataOutput;
import java.io.DataInput;
import java.lang.Math;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class Point2D implements WritableComparable {
  private int x;
  private int y;
  private int z;

  public Point2D(int x, int y, int z) {
    this.x = x;
    this.y = y;
    this.z = z;
  }

  public Point2D() {
    this(0, 0, 0);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(x);
    out.writeInt(y);
    out.writeInt(z);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    x = in.readInt();
    y = in.readInt();
    z = in.readInt();
  }

  @Override
  public String toString() {
    return Integer.toString(x) + ", " + Integer.toString(y) + ", " + 
           Integer.toString(z);
  }

 /** return the Euclidean distance from (0, 0) */
  public int distanceFromOrigin() {
    return (int) Math.sqrt(x*x + y*y + z*z);
  }

  public double distance(Point2D obj){

    int xDiff = this.x - obj.x;
    int yDiff = this.y - obj.y;
    int zDiff = this.z - obj.z;

    return  Math.sqrt(Math.pow(xDiff,2) + Math.pow(yDiff,2) +
                     Math.pow(zDiff,2));
  } 

  @Override
  public int compareTo(Object other) {
    int myDistance = distanceFromOrigin();
    Point2D obj = (Point2D) other;
    int otherDistance = obj.distanceFromOrigin();

    return Integer.compare(myDistance, otherDistance);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }
    if( this == obj){
      return true;
    }

    Point2D other = (Point2D) obj;
    return this.x == other.x && this.y == other.y && this.z == other.z;        
  }

  public int hashCode() {
    return x ^ y ^ z;
  }

  public int getX(){
    return this.x;
  }

  public int getY(){
    return this.y;
  }

  public int getZ(){
    return this.z;
  }

}
