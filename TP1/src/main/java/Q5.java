import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class Q5 {
    /*
     copy the files inside /BDCC/CPP/Cours to
     */
    public static void main(String[] args) {
        Configuration conf = ConnectionToHDFS.getConf();
        FSDataInputStream fsdi;
        FSDataOutputStream fsdo;

        try{
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path Cours1CPP = new Path("/BDCC/CPP/Cours/Cours1CPP"),
                 Cours2CPP = new Path("/BDCC/CPP/Cours/Cours2CPP"),
                 Cours3CPP = new Path("/BDCC/CPP/Cours/Cours3CPP"),
                 Cours1JAVA = new Path("/BDCC/JAVA/Cours/Cours1CPP"),
                 Cours2JAVA = new Path("/BDCC/JAVA/Cours/Cours2CPP"),
                 Cours3JAVA = new Path("/BDCC/JAVA/Cours/Cours3CPP");


            //copying first file
            fsdi=fs.open(Cours1CPP);
            fsdo=fs.create(Cours1JAVA);
            IOUtils.copyBytes(fsdi,fsdo,conf);
            System.out.println("[i] File copied successfully from : /BDCC/CPP/Cours/Cours1CPP to /BDCC/JAVA/Cours/Cours1CPP ✅");
            fsdi.close();
            fsdo.close();

            //copying second file
            fsdi=fs.open(Cours2CPP);
            fsdo=fs.create(Cours2JAVA);
            IOUtils.copyBytes(fsdi,fsdo,conf);
            System.out.println("[i] File copied successfully from : /BDCC/CPP/Cours/Cours2CPP to /BDCC/JAVA/Cours/Cours2CPP ✅");
            fsdi.close();
            fsdo.close();

            //copying third file
            fsdi=fs.open(Cours3CPP);
            fsdo=fs.create(Cours3JAVA);
            IOUtils.copyBytes(fsdi,fsdo,conf);
            System.out.println("[i] File copied successfully from : /BDCC/CPP/Cours/Cours3CPP to /BDCC/JAVA/Cours/Cours3CPP ✅");
            fsdi.close();
            fsdo.close();

            System.out.println("[i] ✅ ALL files have been copied successfully ✅✅✅");

        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }


    }

}
