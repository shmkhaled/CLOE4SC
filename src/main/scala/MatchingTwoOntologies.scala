import org.apache.spark.rdd.RDD
/*
* Created by Shimaa 7.11.2018
* */
class MatchingTwoOntologies {
  def Match(sourceSubOntology: RDD[(String, String, String)], targetOntology: RDD[(String, String, String)], targetClassesWithoutURIs: RDD[String]): RDD[(String, String, String, Char)] ={
    /*
    * divide the source ontology into two sub-ontologies, the first one with all triples that have pridicate 'type', i.e. classes. The second one has the rest*/
    var matchOntology: RDD[(String, String, String)] = sourceSubOntology.intersection(targetOntology)
//      matchOntology.foreach(println(_))
//    println("|        The source ontologyTriples after removing the common triples       |")
    var sourceOntology: RDD[(String, String, String)] = sourceSubOntology.subtract(matchOntology).filter(x=>x._2 != "type")//remove full matched triples from the source ontologyTriples
//    sourceOntology.foreach(println(_))

    var tripelsForEnrichment: RDD[(String, String, String, Char)] = sourceOntology.keyBy(_._1).join(targetClassesWithoutURIs.zipWithIndex().keyBy(_._1)).map({case(a,((s,p,o),b))=> (s,p,o,'E')})
//    println("##############################################")
    var tiplesForAdd = tripelsForEnrichment.keyBy(_._3).join(sourceSubOntology.keyBy(_._1)).map({case(a,((b,c,d,e),(s,p,o)))=> (s,p,o,'A')}).distinct().cache()
//    tiplesForAdd.foreach(println(_))

    tripelsForEnrichment = tripelsForEnrichment.union(tiplesForAdd)

    //.map({case(a,((s,p,o),b))=> if(!a.isEmpty()) (s,p,o,'E') else if (a.isEmpty()) (s,p,o,'A')})
    tripelsForEnrichment
  }

}
