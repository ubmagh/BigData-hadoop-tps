import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;

public class Q3 {

    /*
    Create 3 files Cours{1,2,3}CPP with content inside of /BDCC/CPP/Cours
     */
    public static void main(String[] args) {

        Configuration conf = ConnectionToHDFS.getConf();
        BufferedWriter bw;
        FSDataOutputStream fsdo;

        try{
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path Cours1CPP = new Path("/BDCC/CPP/Cours/Cours1CPP"),
                 Cours2CPP = new Path("/BDCC/CPP/Cours/Cours2CPP"),
                 Cours3CPP = new Path("/BDCC/CPP/Cours/Cours3CPP");


            fsdo = fs.create(Cours1CPP);
            bw = new BufferedWriter(new OutputStreamWriter(fsdo));
            bw.write("Cours1CPP : this is the content of the first file ");
            bw.newLine();
            bw.write("yeah !");
            bw.close();
            fsdo.close();
            System.out.println("[i] file created : /BDCC/CPP/Cours/Cours1CPP ✅");


            fsdo = fs.create(Cours2CPP);
            bw = new BufferedWriter(new OutputStreamWriter(fsdo));
            bw.write("Cours2CPP : this is the content of the second file ");
            bw.newLine();
            bw.write("yeah !!");
            bw.close();
            fsdo.close();
            System.out.println("[i] file created : /BDCC/CPP/Cours/Cours2CPP ✅");

            fsdo = fs.create(Cours3CPP);
            bw = new BufferedWriter(new OutputStreamWriter(fsdo));
            bw.write("Cours3CPP : this is the content of the third file ");
            bw.newLine();
            bw.write("yeah !!!");
            bw.close();
            fsdo.close();
            System.out.println("[i] file created : /BDCC/CPP/Cours/Cours3CPP ✅");

        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }


    }

}
