import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

public class Q4 {

    /*
     display /BDCC/CPP/Cours files content
     */
    public static void main(String[] args) {
        Configuration conf = ConnectionToHDFS.getConf();
        BufferedReader br;
        FSDataInputStream fsdi;
        String out="";

        try{
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path Cours1CPP = new Path("/BDCC/CPP/Cours/Cours1CPP"),
                 Cours2CPP = new Path("/BDCC/CPP/Cours/Cours2CPP"),
                 Cours3CPP = new Path("/BDCC/CPP/Cours/Cours3CPP");


            fsdi = fs.open(Cours1CPP);
            br = new BufferedReader(new InputStreamReader(fsdi));
            System.out.println("\n\n[start] content of the file : /BDCC/CPP/Cours/Cours1CPP : ");
            while( (out = br.readLine())!=null ){
                System.out.println(out);
            }
            br.close();
            fsdi.close();
            System.out.println("[end] content of the file : /BDCC/CPP/Cours/Cours1CPP ------ ");


            fsdi = fs.open(Cours2CPP);
            br = new BufferedReader(new InputStreamReader(fsdi));
            System.out.println("\n\n[start] content of the file : /BDCC/CPP/Cours/Cours2CPP : ");
            while( (out = br.readLine())!=null ){
                System.out.println(out);
            }
            br.close();
            fsdi.close();
            System.out.println("[end] content of the file : /BDCC/CPP/Cours/Cours2CPP ------ ");


            fsdi = fs.open(Cours3CPP);
            br = new BufferedReader(new InputStreamReader(fsdi));
            System.out.println("\n\n[start] content of the file : /BDCC/CPP/Cours/Cours3CPP : ");
            while( (out = br.readLine())!=null ){
                System.out.println(out);
            }
            br.close();
            fsdi.close();
            System.out.println("[end] content of the file : /BDCC/CPP/Cours/Cours3CPP ------");


        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }


    }

}
