package Partie1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class Q3 {

    public static void main(String[] args) {

        SparkSession ss=SparkSession.builder().
                appName("Q3) Afficher pour chaque projet les tâches en retard (avec la durée de retard).").
                master("local[*]").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://192.168.67.1:3306/db_imomaroc");

        options.put("user","root");
        options.put("password","");


        System.out.println(" Q3) Afficher pour chaque projet les tâches en retard (avec la durée de retard). ");
        Dataset<Row> ProjetsEnCours =ss.read().
                format("jdbc")
                .options(options)
                .option("query",
                        " SELECT p.id_projet AS ID_PROJET, p.titre AS TITRE_PROJET, t.id_tache AS ID_TACHE_RETARD, t.titre AS TITRE_TACHE_RETARD, TIMESTAMPDIFF( DAY, t.date_fin, CURRENT_DATE) as DUREE_RETARD_EN_JOURS " +
                                "FROM projets p, taches t " +
                                "WHERE p.id_projet=t.id_projet AND t.date_fin<CURRENT_DATE AND t.terminee=0 ")
                .load();
        ProjetsEnCours.show();





    }

}
