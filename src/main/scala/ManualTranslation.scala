import org.apache.jena.graph
import org.apache.spark.rdd.RDD

class ManualTranslation {
  def Translate (translatedSourceOntology: RDD[(String, String, String)], translations: RDD[graph.Triple]):RDD[(String, String, String)]={
    var trans: RDD[(String, String)] = translations.map(x=>(x.getSubject.getLocalName,x.getObject.getLocalName))
    var t1: RDD[(String, String, String)] = translatedSourceOntology.keyBy(_._1).leftOuterJoin(trans).map{case(x)=> if (x._2._2.isEmpty)(x._2._1) else (x._2._2.last,x._2._1._2,x._2._1._3)}
//    println("after translating all subjects")
//    t1.foreach(println(_))
    var triplesWithSynonyms: RDD[(String, String, String)] = t1.filter(x=>x._2 == "Synonym")
    var t2 = t1.subtract(triplesWithSynonyms)
//    println("#######################")
//    t2.foreach(println(_))

    var t3 = t1.keyBy(_._3).join(trans).map({
      case (g, ((s, pre, o), e)) => (s, pre, e)
    }).union(t2)
//    println("#######################")
//    t3.foreach(println(_))
    t3
  }

}
