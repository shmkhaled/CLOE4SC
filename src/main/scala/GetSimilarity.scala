import info.debatty.java.stringsimilarity.{Cosine, Jaccard, NormalizedLevenshtein}
/*
* Created by Shimaa 6.11.2018
* */

class GetSimilarity extends Serializable{
//  def getExactSimilarity(translatedClasses: RDD[(String, List[String])], targetClasses: RDD[String]): Unit ={
//    var sim: RDD[String] = translatedClasses.flatMap(s => s._2)
//    println("Source classes before join")
//    sim.foreach(println(_))
//    var j: RDD[String] = sim.intersection(targetClasses)
//    println("After join")
//    j.foreach(println(_))
//
//  }
  def getSimilarity(s1: String, s2: String): Double={
    val cos = new Cosine(2)
    var cosSim = cos.similarity(s1, s2)
//    System.out.println("Cosine similarity is "+cosSim)
  import info.debatty.java.stringsimilarity.JaroWinkler
  val jw = new JaroWinkler
  var jarSim = jw.similarity(s1, s2)

    val l = new NormalizedLevenshtein()
    var levenshteinSim = l.distance(s1, s2)
//    System.out.println("Normalized Levenshtein similarity is "+levenshteinSim)

    val j = new Jaccard(3)
    var jaccardSim = j.similarity(s1, s2)
//    System.out.println("Jaccard similarity is "+jaccardSim)

    import info.debatty.java.stringsimilarity.NGram
    val trigram = new NGram(3)
    var trigramSim = trigram.distance(s1, s2)
//    System.out.println("trigram similarity is "+trigram.distance(s1, s2))

  jaccardSim

  }
}
