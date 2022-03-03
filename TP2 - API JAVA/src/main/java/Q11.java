import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Q11 {
    /*
        delete the file /BDCC/CPP/TPs/TP1CPP
     */
    public static void main(String[] args) {
        Configuration conf = ConnectionToHDFS.getConf();

        try{
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path file = new Path("/BDCC/CPP/TPs/TP1CPP");

            if ( fs.delete( file, true) )
                System.out.println("[i]  file deleted ✅");
            else
                System.out.println("[i]  can't delete file  ❌");

        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }
    }
}
