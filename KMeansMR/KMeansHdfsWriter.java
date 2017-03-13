package KMeansMR;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.util.Scanner;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

public class KMeansHdfsWriter {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length < 2) {
            System.err.println("HdfsWriter [local input path] [hdfs output path]");
            System.exit(0);
        }
        
        String localInputPath = args[0];
        Path outputPath = new Path(args[1]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            System.out.println("output path exists---Deleting it");
            fs.delete(outputPath, true);
        }
       
        Scanner sc = new Scanner (new File(localInputPath));

        SequenceFile.Writer out = SequenceFile.createWriter(fs,
                                         conf, outputPath,
                                        Point2D.class, Point2D.class);
        while(sc.hasNextInt()){
          out.append(new Point2D(0,0,0), new Point2D(sc.nextInt(),sc.nextInt(),sc.nextInt()));
        }
    }
}
