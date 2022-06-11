package Partie1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class Q2 {

    public static void main(String[] args) {

        SparkSession ss=SparkSession.builder().
                appName("Q2)Afficher pour chaque projet, le nombre de tâches dont la durée dépasse un mois").
                master("local[*]").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://192.168.67.1:3306/db_imomaroc");

        options.put("user","root");
        options.put("password","");



        System.out.println(" Q2) Afficher pour chaque projet, le nombre de tâches dont la durée dépasse un mois ");
        Dataset<Row> ProjetsEnCours =ss.read().
                format("jdbc")
                .options(options)
                .option("query",
                        " SELECT p.id_projet AS ID_PROJET, p.titre AS TITRE, count(p.id_projet) AS NOMBRE_TACHES_DEPASSENT_1_MOIS " +
                                "FROM projets p, taches t " +
                                "WHERE p.id_projet=t.id_projet AND TIMESTAMPDIFF( MONTH, t.date_debut , t.date_fin)>1 " +
                                "GROUP BY p.id_projet  ")
                .load();
        ProjetsEnCours.show();





    }

}
