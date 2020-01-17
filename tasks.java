import org.apache.spark.sql.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import scala.collection.Iterator;
import java.util.Date;

import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.stream.Stream;

import static java.lang.Math.abs;
import static org.apache.spark.sql.functions.*;

/*
1. Используя данные из файла energy-usage-2010.csv рассчитать суммарное потребление энергии по месяцам
(KWH JANUARY 2010, KWH FEBRUARY 2010 и т.д.) в разрезе территорий (COMMUNITY AREA NAME)
 */
public class tasks {
    public void energy_count(){
        HashMap<String,String> exps = new HashMap<>();
        for(int i=4;i<16;i++){
            String temp = "_c";
            exps.put(temp+i, "sum");
        }
        SparkSession sp = SparkSession.builder()
                .appName("Energy count")
                .master("local")
                .getOrCreate();
        Dataset<Row> ds = sp.read()
                .format("csv")
                .load("input/energy-usage-2010.csv");
        Dataset<Row> electr_ds = ds.groupBy("_c0").agg(exps);
        electr_ds.show(100);
    }

    /*
    2. Используя файл RUvideos.csv найти 10 наиболее популярых видое (по просмотрам)
     */
    public void top_videos(){
        SparkSession sp = SparkSession
                .builder()
                .appName("Top videos")
                .master("local")
                .getOrCreate();
        Dataset<Row> ds = sp.read()
                .format("csv")
                .load("input/RUvideos.csv");
        Dataset<Row> TopTen = ds.select(col("_c2"),  col("_c7").cast(DataTypes.IntegerType)).sort(desc("_c7"));
        TopTen.show();
    }

    /*
    3. Используя файл RUvideos.csv найти наиболее популярные видео (по просмотрам) в разрезе месяцев
     */
    public void top_videos_per_month(){
        HashMap<String,String> exps = new HashMap<>();
        exps.put("_c7","max");
        exps.put("_c2","");

        SparkSession sp = SparkSession
                .builder()
                .appName("Top videos")
                .master("local")
                .getOrCreate();
        Dataset<Row> ds = sp.read()
                .format("csv")
                .load("input/RUvideos.csv");
        Dataset<Row> date = ds.withColumn("month_year", new Column("_c5").substr(1,7));
        date = date.select("_c2", "_c7", "month_year");
        Dataset<Row> TopTen = date.groupBy("month_year").agg(exps);
        //TopTen.groupBy("month_year");
        TopTen.show(100);
    }

    /*
    4. Рассчитать среднее время исполнения заявки в разрезе страховых компаний
     */
    public void claimPerformCount(){
        HashMap<String,String> agrg = new HashMap<>();
        agrg.put("_c16", "sum");
        SparkSession sp = SparkSession
                .builder()
                .appName("claimPerformCount")
                .master("local")
                .getOrCreate();
        Dataset<Row> ds = sp.read()
                .format("csv")
                .load("input/data-1542534337679.csv");
        ds = ds.select("_c0", "_c1", "_c16");
        ds = ds.groupBy("_c0", "_c1").agg(agrg);

        ds.show(10);
    }

    /*
    5. На основании набора (event_data_train.csv) найти всех пользователей, которые полностью прошли курс
    */
    public void passedCourceCount(){
        HashMap<String,String> hm = new HashMap<>();
        hm.put("_c0","count");
        SparkSession sp = SparkSession
                .builder()
                .appName("passedCourceCount")
                .master("local")
                .getOrCreate();
        Dataset<Row> ds = sp.read()
                .format("csv")
                .load("input/event_data_train.csv");
        ds = ds.select("_c0", "_c2", "_c3").where("_c2='passed'");
        ds = ds.groupBy("_c3").agg(hm).where("count(_c0)=198");
        ds.show(100);
    }

    /*
       6. На основании набора (submissions_data_train.csv) найти сколько задач
       успешно решили пользователи, которые прошли курс полностью
        */
    public void passedCourceCountTask(){
        HashMap<String,String> hm = new HashMap<>();
        HashMap<String,String> hm2 = new HashMap<>();
        hm.put("_c0","count");
        hm2.put("_c2", "count");

        SparkSession sp1 = SparkSession
                .builder()
                .appName("passedCourceCount")
                .master("local")
                .getOrCreate();
        SparkSession sp2 = SparkSession
                .builder()
                .appName("passedCourceCountTask")
                .master("local")
                .getOrCreate();

        Dataset<Row> ds = sp1.read()
                .format("csv")
                .load("input/event_data_train.csv");
        Dataset<Row> ds2 = sp2.read()
                .format("csv")
                .load("input/submissions_data_train.csv");

        ds = ds.select("_c0", "_c2", "_c3").where("_c2='passed'");
        ds = ds.groupBy("_c3").agg(hm).where("count(_c0)=198");
        ds = ds.withColumnRenamed("_c3", "user_id");
        ds2 = ds2.select("_c2", "_c3").where("_c2='correct'");
        ds2 = ds2.join(ds, ds2.col("_c3").equalTo(ds.col("user_id")), "inner" );
        ds2= ds2.groupBy("user_id").agg(hm2);
        ds2.show(100);
    }

    /*
    7. Найти id автора курса
     */
    public void findAdmin(){
        HashMap<String ,String> hm = new HashMap<>();
        hm.put("_c2", "count");
        SparkSession sp = SparkSession
                .builder()
                .appName("findAdmin")
                .master("local")
                .getOrCreate();
        Dataset<Row> ds = sp.read()
                .format("csv")
                .load("input/submissions_data_train.csv");
        ds = ds.select("_c0", "_c2", "_c3").where("_c2='correct'");
        ds= ds.groupBy("_c0", "_c3").agg(hm).where("count(_c2)>1");
        ds = ds.select(col("_c0"), col("_c3"), col("count(_c2)").cast(DataTypes.IntegerType)).sort(desc("count(_c2)"));
        ds.show(100);
    }

    /*
    8. Для каждого пользователя найдите такой шаг, который он не смог решить,
    и после этого не пытался решать другие шаги. Затем найдите id шага,
    который стал финальной точкой практического обучения на курсе для максимального числа пользователей.
     */
    public void stopPoint(){
        HashMap<String, String > hm = new HashMap<>();
        HashMap<String, String > hm2 = new HashMap<>();
        hm.put("_c1", "max");
        hm2.put("_c3", "count");
        SparkSession sp = SparkSession
                .builder()
                .appName("stopPoint")
                .master("local")
                .getOrCreate();
        Dataset<Row> ds = sp.read()
                .format("csv")
                .load("input/submissions_data_train.csv");
        ds=ds.select("*").where("_c2='wrong'");
        ds= ds.groupBy("_c0", "_c3").agg(hm);

        Dataset<Row> ds2 = ds.groupBy("_c0").agg(hm2);
        ds2= ds2.select(col("_c0"), col("count(_c3)").cast(DataTypes.IntegerType)).sort(desc("count(_c3)"));
        ds2.show(10);
    }




}
