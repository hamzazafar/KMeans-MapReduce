package KMeansMR;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class KMeansDriver {

  private static final Log logger = LogFactory.getLog(KMeansDriver.class);
  private static int iteration = 0;
  public static void main(String [] args){
 
   try{
 
    // Path input = new Path("/KMeans/input/inData");
     Path input = new Path(args[0]);
     Path output = new Path(args[2]+iteration);
     Path randomCenters = new Path(args[1]);

     Configuration conf = new Configuration();
     conf.set("random.centers",randomCenters.toString());
     conf.set("output", output.toString());

     FileSystem fs = FileSystem.get(conf);
     cleanUpFiles(output,fs);
     cleanUpFiles(randomCenters,fs);

     writeRandomCenters(conf,randomCenters,fs);


     Job job = Job.getInstance(conf);
     job.setJarByClass(KMeansMapper.class);
     job.setMapperClass(KMeansMapper.class);
     job.setReducerClass(KMeansReducer.class);

     job.setJobName("KMeans-Clustering-"+iteration);

     FileInputFormat.addInputPath(job,input);
     FileOutputFormat.setOutputPath(job,output);
    
     job.setInputFormatClass(SequenceFileInputFormat.class);
     job.setOutputFormatClass(SequenceFileOutputFormat.class);

     job.setOutputKeyClass(Point2D.class);
     job.setOutputValueClass(Point2D.class);
     job.setNumReduceTasks(1);
     // Submit the job, then poll for progress until the job is complete
     job.waitForCompletion(true);
     //printIterationResults(output,fs,conf);
     logger.info("INPUT " + input.toString());
     logger.info("OUTPUT " + output.toString());
     iteration++;
     while(job.getCounters().findCounter(KMeansReducer.Counter
					 .UPDATED_CENTERS).getValue() > 0){
       conf = new Configuration();
       conf.set("random.centers", randomCenters.toString());

       job = Job.getInstance(conf);
       job.setJobName("KMeansClustering-" + iteration);

       job.setMapperClass(KMeansMapper.class);
       job.setReducerClass(KMeansReducer.class);
       job.setJarByClass(KMeansMapper.class);
 
        input = new Path(args[2] + (iteration - 1)+"/");
        FileInputFormat.addInputPath(job, input);
       output = new Path(args[2] + iteration);
       for(int i=0;i<(iteration-2);i++){if(fs.exists(new Path(args[2]+i))){fs.delete(new Path(args[2]+i));  }}
       conf.set("output", output.toString());
      

       if (fs.exists(output))
         fs.delete(output, true);

     logger.info("INPUT " + input.toString());
     logger.info("OUTPUT " + output.toString());

       FileOutputFormat.setOutputPath(job, output);
       job.setInputFormatClass(SequenceFileInputFormat.class);
       job.setOutputFormatClass(SequenceFileOutputFormat.class);
       job.setOutputKeyClass(Point2D.class);
       job.setOutputValueClass(Point2D.class);
       job.setNumReduceTasks(1);
       job.waitForCompletion(true);
      // printIterationResults(fs,conf);
       iteration++;
     }
   }catch(Exception exp){
     exp.printStackTrace();
   }
   
  }

  public static void cleanUpFiles(Path p, FileSystem fs) throws IOException{
    if(fs.exists(p)){
      fs.delete(p,true);
    }     
  }
 
  public static void printIterationResults(FileSystem fs ,
                                      Configuration conf) throws IOException{
    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                                      new Path(conf.get("output")) , conf)) {
                        Point2D key = new Point2D();
                        Point2D value = new Point2D();
                        while (reader.next(key,value)) {
                           System.out.println("Key: "+key.toString() +
                                       " / Value: "+value.toString());
                        }
                }

  }

  public static void writeRandomCenters(Configuration conf, Path randomCenters,
                                             FileSystem fs) throws IOException{

    try (SequenceFile.Writer randomWriter = SequenceFile.createWriter(
                                            fs, conf, randomCenters, 
                                            Point2D.class, Point2D.class)) {

       randomWriter.append(new Point2D(234,123,345 ), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(789,567,435 ), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(111,0,122 ), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(222,345,321), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(333,123,100), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(444,345,345), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(100,123,234), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(200,322,342), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(300,222,432), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(400,444,432), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(500,555,534), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(121,432,134), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(221,324,122), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(334,441,132), new Point2D(0, 0, 0));
       randomWriter.append(new Point2D(432,432,100), new Point2D(0, 0, 0));


    }
  }  
  public static void mergeReducerFiles(Configuration conf, Path out,String in,
                              FileSystem fs, int itr) throws IOException{
   FileStatus[] fileStatus =fs.listStatus(new Path("/KMeansMR/center"+itr+"/"));
    //4. Using FileUtil, getting the Paths for all the FileStatus
    Path[] paths = FileUtil.stat2Paths(fileStatus);
    System.out.println("Total paths"+paths.length);
    ArrayList<Point2D> arr = new ArrayList<Point2D>();
    for(Path p :paths){
      System.out.println(p);
      try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, 
                                              p, conf)) {
        Point2D key = new Point2D();
        Point2D value = new Point2D();

        while (reader.next(key,value)) {
          arr.add(key);
        }
      }
    }
  fs.delete(out);
  System.out.println("Adding "+arr.size());
  try (SequenceFile.Writer randomWriter = SequenceFile.createWriter(
                                            fs, conf, out,
                                            Point2D.class, Point2D.class)) {

       for(Point2D key : arr){
         randomWriter.append(key, new Point2D(0, 0, 0));
       }

    }


  }
  public static void writeInputFile(Configuration conf, Path input,
                                       FileSystem fs) throws IOException{
    try (SequenceFile.Writer inputWriter = SequenceFile.createWriter(
                                            fs, conf, input,
                                            Point2D.class, Point2D.class)) {

       inputWriter.append(new Point2D(0, 0, 0), new Point2D(0, 10, 100));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(0, 50, 0));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(10, 10, 10));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(10, 30, 25));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(20, 90, 45));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(20, 0, 20));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(30, 30, 30));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(30, 70, 30));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(40, 10, 20));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(40, 10, 40));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(50, 30, 20));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(50, 60, 70));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(60, 70, 80));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(60, 20, 20 ));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(70, 90, 100 ));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(70, 60, 30));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(80, 40, 20));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(80, 90, 70));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(90, 40, 30));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(90, 90, 90));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(10, 15, 20));
       inputWriter.append(new Point2D(0, 0, 0), new Point2D(10, 20 ,30));

    }
  }
}
