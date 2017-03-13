import java.util.Random;

import java.io.File;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.FileOutputStream;

public class GenerateData{
  public static void main(String [] args)throws IOException{
    Random random = new Random();
    FileOutputStream fos = null;
    DataOutputStream dos = null;
    try{
    fos  = new FileOutputStream(args[1]);
    dos = new DataOutputStream(fos);
    int size = Integer.parseInt(args[0]);

    for(int i=0;i<size;i++){
      dos.writeInt(random.nextInt(500));
      dos.writeInt(random.nextInt(500));
      dos.writeInt(random.nextInt(500));
      dos.flush();
    }
    }
    catch(Exception exp){
      exp.printStackTrace();
    }
    finally{
     fos.close();
     dos.close();
    
    }
  }
}
