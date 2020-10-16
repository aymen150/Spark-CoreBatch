package fr.esgi.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object DataFrame3 {
  def main(args: Array[String]): Unit = {

    //TP3
    //1
    // famille arbre
    // Dataset contient 2 colonnes. Une avec les genres d'arbres et l'autre avec leur famille
    // abrealignementdansparis
    // fichier contenant la position précise de différent arbre dans paris.
    // une union semble etre possible entre les deux datasets


    //Instancier le spark session
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("trees")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)
  // 2
      val tress = sc
        .textFile("../../../../resources/data/arbresalignementparis2010.csv")
        .filter(line ⇒ !line.startsWith("geom"))
        .map(line ⇒ line.split(";"))

       val nb_arbre_paris= tress.count()
       print(nb_arbre_paris)

    //3

    val first_arbre = tress
       .zipWithIndex()
       .filter(x=> x._2 !=0)
       .map(col => col._1(2))
       .filter(line => line != "")
       .take(20)
    first_arbre.foreach(println)
    //4

    val all_arbre= tress
      .zipWithIndex()
      .filter(x=> x._2 !=0)
      .map(col => col._1(2))
      .filter(line => line != "")
      .distinct()

      all_arbre.foreach(println)

    // 5
    val sum_arbre= tress
      .zipWithIndex()
      .filter(x=> x._2 !=0)
      .map(col => col._1(7).toFloat)
      .filter(line => line != "")
      .sum()
println("question 5:",sum_arbre)

    //6
    val reduce_arbre= tress
      .zipWithIndex()
      .filter(x=> x._2 !=0)
      .map(col => col._1(7).toFloat)
      .reduce((size,a)=>(size+a))
   // println("question 6:", reduce_arbre)
  //7

  val mean_arbre= tress
    .zipWithIndex()
    .filter(x=> x._2 !=0)
    .map(col => col._1(7).toFloat)
    .mean()

    println("question 7.1:", mean_arbre)

    println("question 7.2 :", reduce_arbre/nb_arbre_paris)
    //2
//8

val type_arbre= tress
  .zipWithIndex()
  .filter(x=> x._2 !=0)
  .map(col => col._1(2))
  .countByValue()

    println("question 8:",type_arbre)

    //9
    val red_type_arbre= tress
      .zipWithIndex()
      .filter(x=> x._2 !=0)
      .map(col =>( col._1(2),1))
      .reduceByKey((value, key) => value + key)
      .sortByKey(true)

     red_type_arbre.foreach(println)

    // 10
    val famille = sc
      .textFile("../../../../resources/data/familles d'arbes.csv")
      .filter(line ⇒ !line.startsWith("geom"))
      .map(line ⇒ line.split(";"))

   val fus_1= tress
     .zipWithIndex()
     .filter(x=> x._2 !=0)
     .map(col =>( col._1(2),1))

    val fus_2= famille
      .zipWithIndex()
      .filter(x => x._2 !=0)
      .map(col => (col._1(0),col._1(1)))

    val fusion = fus_1.join(fus_2)

    val famille_fusion = fusion
      .map(col => col._2._2)

    val family_fus = famille_fusion
        .countByValue()


    //11

    val familyCollectAsMap= famille
      .map(col => (col(0),col(1)))
      .collectAsMap()
    val broadcastFamily = sc.broadcast(familyCollectAsMap)

    val custom_tree= tress
      .filter(f=>f(2) != "")
      .map(col =>  broadcastFamily.value(col(2)))
      .countByValue()
     custom_tree.foreach(println)








  }

}