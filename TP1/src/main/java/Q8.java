import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Q8 {
    /*
        Copy files TP1CPP & TP2CPP from local directory /Q7 to remote directory /BDCC/CPP/TPs
     */
    public static void main(String[] args) {
        Configuration conf = ConnectionToHDFS.getConf();

        try{
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path TP1r = new Path("/BDCC/CPP/TPs/TP1CPP"),
                 TP2r = new Path("/BDCC/CPP/TPs/TP2CPP"),
                 TP1l = new Path("./Q7/TP1CPP"),
                 TP2l = new Path("./Q7/TP2CPP");

            fs.copyFromLocalFile( TP1l, TP1r);
            System.out.println("[i] file TP1CPP copied ✅");

            fs.copyFromLocalFile( TP2l, TP2r);
            System.out.println("[i] file TP2CPP copied ✅");

        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }


    }

}
