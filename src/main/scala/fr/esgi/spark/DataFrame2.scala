package fr.esgi.spark

import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._

object DataFrame2 {

  def main(args: Array[String]): Unit = {

    //Instancier le spark session
    val spark = SparkSession.builder()
      .appName("Spark SQL TD1")
      .config("spark.driver.memory","512m")
      .master("local[4]")
      .getOrCreate()

    val reader = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter",";")
      .format("csv")
      .load("../../../../resources/data/laposte_hexasmal.csv")
    reader.show()
    //Code_commune_INSEE|Nom_commune|Code_postal|Libelle_acheminement| Ligne_5|coordonnees_gps|
    // TP1

     val  nb_commune= reader
      .select("Nom_commune")
      .count()
    val df_without_list_5 = reader.where(col("Ligne_5") =!= "")
    val df_with_dept = reader.withColumn("dep",substring(col("Code_commune_INSEE"),0,2))
    print("nombre de commune :", nb_commune)

    // TD2
    //1.
    def new_departement (arg1:String)={  arg1.substring(0,2) }
    def new_dep= udf(new_departement _)

    //2.
    val new_data = reader.withColumn("dep", new_dep(col("Code_commune_INSEE") ) )
    new_data.show

    //3.
    val tmp1 = new_data
      .groupBy(col("dep"))
      .agg(countDistinct("Code_postal"))

    val tmp2 = tmp1.agg(max("count(DISTINCT Code_postal)"))

    val departement_max = tmp1.join(tmp2,tmp1("count(DISTINCT Code_postal)") === tmp2("max(count(DISTINCT Code_postal))"),"inner")
    departement_max.select(col="dep","count(DISTINCT Code_postal)" ).show()

    //4.
    val departement_mean = tmp1.agg(mean("count(DISTINCT Code_postal)"))
    departement_mean.show()

    //5
    val data_fichier_departement = spark.read
      .option("inferSchema", "true")
      .option("header", "false")
      .option("index","true")
      .option("delimiter",",")
      .format("csv")
      .load("../../../../resources/data/departement.csv")

    // Il ne contient pas de header. Il a une colonne departement comme la colonne que nous avons ajouté au data du fichier 1.
    // Il contient une ligne pour chaque departement. Une intersection avec le premier fichier semble etre possible pour avec un dataframe plus complet
    data_fichier_departement.show()

    //6
    val data_intersection = new_data.join(data_fichier_departement,data_fichier_departement("_c1")=== new_data("dep"),"inner")
    data_intersection.show

    //7
     data_intersection.write.option("header", "true").csv("../../../../resources/output/outputTD2.csv")

    //8
    data_fichier_departement.createOrReplaceTempView("data1")
    new_data.createOrReplaceTempView("data2")

    val sql_variable = spark.sql("""
      |SELECT *
      |FROM data1
      |INNER JOIN data2
      |ON data1._c1 = data2.dep
      """.stripMargin)

    sql_variable.show()
  }
}