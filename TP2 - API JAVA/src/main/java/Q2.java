import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Q2 {

    /*
        following code will create the tree bellow inside th fs of hadoop :
            /
            |---/BDCC
                |-----/JAVA
                      |---/TSPs
                      |---/Cours
                |-----/CPP
                      |---/TPs
                      |---/Cours
         */
    public static void main(String[] args) {
        Configuration conf = ConnectionToHDFS.getConf();
        try {
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path javaTpsPath = new Path("/BDCC/JAVA/TPs"),
                 javaCrsPath = new Path("/BDCC/JAVA/Cours"),
                 cppCoursPath = new Path("/BDCC/CPP/Cours"),
                 cppTpsPath = new Path("/BDCC/CPP/TPs");
            System.out.println("[i] creating tree : '/BDCC/..' ... ");

             if( fs.mkdirs(cppCoursPath) )
                 System.out.println("[i] path created : /BDCC/CPP/Cours ✅");
             else
                 System.out.println("[i]  can't create path : /BDCC/CPP/Cours ❌");

            if( fs.mkdirs(cppTpsPath) )
                System.out.println("[i] path created : /BDCC/CPP/TPs ✅");
            else
                System.out.println("[i]  can't create path : /BDCC/CPP/TPs ❌");

            if( fs.mkdirs(javaCrsPath) )
                System.out.println("[i] path created : /BDCC/JAVA/Cours ✅");
            else
                System.out.println("[i]  can't create path : /BDCC/JAVA/Cours ❌");

            if( fs.mkdirs(javaTpsPath) )
                System.out.println("[i] path created : /BDCC/JAVA/TPs ✅");
            else
                System.out.println("[i]  can't create path : /BDCC/JAVA/TPs ❌");

        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }
        
    }
    
}
