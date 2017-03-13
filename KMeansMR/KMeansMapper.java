package KMeansMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KMeansMapper extends Mapper<Point2D, Point2D, Point2D, Point2D> {

  private static final Log logger = LogFactory.getLog(KMeansMapper.class);
  private final List<Point2D> centers = new ArrayList<Point2D>();

  @Override
  protected void setup(Context context) throws IOException,InterruptedException{
    super.setup(context);

    Configuration conf = context.getConfiguration();
    Path centroids = new Path(conf.get("random.centers"));
   // logger.info("Centroids Path: "+centroids.toString());
    FileSystem fs = FileSystem.get(conf);
                 
    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, 
                                                                      conf)) {
      Point2D key = new Point2D();
      Point2D value = new Point2D();
     
      while (reader.next(key)) {
       // logger.info("Random Center: "+key.toString());
        centers.add(new Point2D(key.getX(),key.getY(), key.getZ()));
      }
    }
  }

  @Override
  protected void map(Point2D key, Point2D value, Context context) 
                                      throws IOException,InterruptedException {

    //logger.info("Got Key: "+key.toString());
    //logger.info("Got Value: "+value.toString());

    Point2D nearest = null;
    double nearestDistance = Double.MAX_VALUE; 

    for (Point2D c : centers) {
      //logger.info("checking distance with: "+c.toString()); 
      double dist = value.distance(c);
      //logger.info("distance: "+dist);
      if (nearest == null) {
        nearest = new Point2D(c.getX(), c.getY(), c.getZ());
	nearestDistance = dist;
      } 
      else if (dist < nearestDistance) {
	 nearest = new Point2D(c.getX(),c.getY(), c.getZ()); 
         nearestDistance = dist;
      }
    }
    //logger.info("New Key: "+nearest.toString());
    context.write(nearest, value);
  }
}
