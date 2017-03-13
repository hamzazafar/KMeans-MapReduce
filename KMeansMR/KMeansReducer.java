package KMeansMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KMeansReducer extends Reducer<Point2D, Point2D, Point2D, Point2D>{

  private static final Log logger = LogFactory.getLog(KMeansReducer.class);

  public static enum Counter{
    UPDATED_CENTERS
  }
  
  private static List<Point2D> centers = new ArrayList<>();

  @Override
  protected void reduce(Point2D key, Iterable<Point2D> values, Context context)                                       throws IOException,InterruptedException {

    //logger.info("Got Key: "+key.toString());
    int x = 0;
    int y = 0;
    int z = 0;
    int size = 0;
    List <Point2D> points = new ArrayList<Point2D>(); 
    for (Point2D value : values) {
      //logger.info("Got Value: "+value.toString());
      x = x + value.getX();
      y = y + value.getY();
      z = z + value.getZ();
      points.add(new Point2D(value.getX(),value.getY(),value.getZ()));
      size++;
    }
   
    Point2D newCenter = new Point2D(x/size , y/size, z/size);
    centers.add(newCenter);
		
    for (Point2D value : points) { 
      context.write(newCenter,value);
    }
   // context.write(new Point2D(10.0,10.0),new Point2D(10.0,10.0));
            
    //logger.info("new Center: "+ newCenter.toString());
    //logger.info("Converg: "+converge(newCenter,key));
    if(converge(newCenter,key) > 1){
      context.getCounter(Counter.UPDATED_CENTERS).increment(1);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void cleanup(Context context) throws IOException,
                                                         InterruptedException {
  //logger.info("Cleaning UP STUFFF");
  super.cleanup(context);
  Configuration conf = context.getConfiguration();
  Path outPath = new Path(conf.get("random.centers"));
  FileSystem fs = FileSystem.get(conf);

  fs.delete(outPath, true);
  try (SequenceFile.Writer out = SequenceFile.createWriter(fs, 
                                           context.getConfiguration(), outPath,
			                       Point2D.class, Point2D.class)) {
    for (Point2D center : centers) {
      out.append(new Point2D(center.getX(),center.getY(), center.getZ()),
                 new Point2D(0, 0, 0));
      logger.info(center.toString());      
    }
  }
  }
   
  protected double converge(Point2D newCenter, Point2D oldCenter){
    return newCenter.distance(oldCenter);
  }
}
