import org.apache.jena.graph
import org.apache.spark.rdd.RDD

class SourceOntologyReconstruction {
  def ReconstructOntology (sourceOntology: RDD[graph.Triple], sourceClassesWithTranslation: RDD[(String, String)]):RDD[(String, String, String)]={
    var sourceOntologyWithoutURI: RDD[(String, String, String)] = sourceOntology.map{case(x)=> if (x.getObject.isLiteral)(x.getSubject.getLocalName,x.getPredicate.getLocalName,x.getObject.getLiteral.toString)else (x.getSubject.getLocalName,x.getPredicate.getLocalName,x.getObject.getLocalName)}

    //        sourceOntologyWithoutURI.foreach(println(_))

    var subjectTranslation: RDD[(String, String, String)] = sourceOntologyWithoutURI.keyBy(_._1).join(sourceClassesWithTranslation)
      .map({
        case (g, ((s, pre, o), e)) => (e, pre, o)
      })//.cache()
    //    println("Source Ontology after translating the subject class")
    //    subjectTranslation.foreach(println(_))
    //
    //            println("#################################################")
    var unTranslatedObjects = subjectTranslation.filter(x=>x._2 == "subClassOf")
    //            println("extracted RDD")
    //            unTranslatedObjects.foreach(println(_))

    var subtractedOntology: RDD[(String, String, String)] = subjectTranslation.subtract(unTranslatedObjects)//.cache()
//    subtractedOntology.foreach(println(_))

    var translatedObjects = unTranslatedObjects.keyBy(_._3.toString).join(sourceClassesWithTranslation)
      .map({
        case (g, ((s, pre, o), e)) => (s, pre, e)
      })//.cache()
//    println("translating the extracted RDD")
//    translatedObjects.foreach(println(_))

    var translatedSourceOntology: RDD[(String, String, String)] = subtractedOntology.union(translatedObjects)//.cache()
//    println("Source Ontology after translating subject and object classes")
//    translatedSourceOntology.foreach(println(_))
    translatedSourceOntology
  }

}
