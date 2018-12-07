import java.util

import de.danielnaber.jwordsplitter.GermanWordSplitter
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import edu.stanford.nlp.tagger.maxent.MaxentTagger


class PreProcessing extends Serializable{
  def ReConstructOntology(ontologyTriples: RDD[graph.Triple]): RDD[(String, String, String)] = {
    var classLabels: RDD[graph.Triple] = ontologyTriples.filter(x=>x.getPredicate.getLocalName == "label")
//    println("classes with labels "+classLabels.count())
//    classLabels.foreach(println(_))

    var ontologyWithSubjectClass: RDD[(Node, Node, Node)] = ontologyTriples.keyBy(_.getSubject).join(classLabels.keyBy(_.getSubject)).map(x=>(x._2._2.getObject,x._2._1.getPredicate,x._2._1.getObject)).filter(x=>x._2.getLocalName != "label")
//    println("After join")
//    ontologyWithSubjectClass.foreach(println(_))

    var ontologyWithSubjectAndObjectClass: RDD[(String, String, String)] = ontologyWithSubjectClass.keyBy(_._3).join(classLabels.keyBy(_.getSubject)).map(x=>(this.stringPreProcessing2(x._2._1._1.toString),x._2._1._2.getLocalName,this.stringPreProcessing2(x._2._2.getObject.toString)))
//var ontologyWithSubjectAndObjectClass: RDD[(String, String, String)] = ontologyWithSubjectClass.keyBy(_._3).join(classLabels.keyBy(_.getSubject)).map(x=>(x._2._1._1.toString,x._2._1._2.getLocalName,x._2._2.getObject.toString))
      //.map(x=>(x._2._2.getObject,x._2._1._2,x._2._1._3))//.filter(x=>x._2.getLocalName() != "label")
//    println("After join")
//    ontologyWithSubjectAndObjectClass.take(5).foreach(println(_))
    ontologyWithSubjectAndObjectClass

  }
  def stringPreProcessing(term: String): String = {
    //For SemSur and Edas and ekaw Datasets
    var preProcessedString: String = term.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim
    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase
     splittedString

    /*
    * for conference and cmt*/
//    var preProcessedString: String = term.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim.toLowerCase
//    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase
    splittedString
//    preProcessedString
  }
  def stringPreProcessing2(term: String): String = {
    /*For SemSur Dataset
    var preProcessedString: String = term.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim
    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase
     splittedString
    * */
    var preProcessedString: String = term.split("@").head.replace("\"", "")//.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim.toLowerCase
    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase
    //    splittedString
    preProcessedString
  }
  def splitCamelCase(s: String): String = {
//    return s.replaceAll(
//      String.format("%s|%s|%s",
//        "(?<=[A-Z])(?=[A-Z][a-z])",
//        "(?<=[^A-Z])(?=[A-Z])",
//        "(?<=[A-Za-z])(?=[^A-Za-z])"
//      ),
//      " "
//    ).replaceAll("  ", " ").split(" ")
    return s.replaceAll(
      String.format("%s|%s|%s",
        "(?<=[A-Z])(?=[A-Z][a-z])",
        "(?<=[^A-Z])(?=[A-Z])",
        "(?<=[A-Za-z])(?=[^A-Za-z])"
      ),
      " "
    ).replaceAll(" ", " ")
  }
  def germanWordSplitter(s: String):util.List[String]={
    val splitter = new GermanWordSplitter(true)
    val parts: util.List[String] = splitter.splitWord(s)
    parts
  }
  def ToCamel(s: String): String = {
    val split = s.split(" ")
    val tail = split.tail.map { x => x.head.toUpper + x.tail }
    split.head.capitalize+ tail.mkString
  }

  def getLastBitFromUrI(urI: String): String = {
//    urI.replaceFirst(".*/([^/?]+).*", "$1")
    urI.replaceFirst(".*/([^/?]+)", "$0")
//    Explanation:
//
//    .*/      // find anything up to the last / character
//    ([^/?]+) // find (and capture) all following characters up to the next / or ?
//    // the + makes sure that at least 1 character is matched
//    .*       // find all following characters
//
//
//    $1       // this variable references the saved second group from above
//    // I.e. the entire string is replaces with just the portion
//    // captured by the parentheses above
  }
  def getURIWithoutLastString(urI: String): String = {
    urI.substring(0,urI.lastIndexOf("/")) + "/"

  }
  def getStringWithoutTags(str: Array[String]): String = {
    str.map(x=>x.split("_").head).mkString(" ")
  }
  def posTag(sourceClassesWithoutURIs: Array[String]): Array[String]={
    var sourceC: Array[String] = sourceClassesWithoutURIs.filter(x => x.split(" ").length == 1)
    var sourceC2 = sourceClassesWithoutURIs diff sourceC
//    println("####################### Subtraction results #######################")
//    sourceC2.foreach(println(_))
    var tagger = new MaxentTagger("/home/shimaa/CL_Enrichment/resources/taggers/german-fast.tagger")
    var tags: Array[String] = sourceC2.map(x=>(tagger.tagString(x).split(" ")).filter(y=> y.contains("_ADJA") || y.contains("_NN")|| y.contains("_XY") || y.contains("_ADV")|| y.contains("_NE")).mkString(" "))
    var removeTags: Array[String] = tags.map(x=>this.getStringWithoutTags(x.split(" ")))
//    println("All Tags")
//    tags.foreach(println(_))
//    println("Removing Tags")
//    removeTags.foreach(println(_))
    var preprocessedSourceClasses: Array[String] = sourceC.union(removeTags)
//    println("All source classes after preprocessing")
//    preprocessedSourceClasses.foreach(println(_))
//    var s: Unit = this.getStringWithoutTags(Array("erweitertes_ADJA","zusammenfassung_NN"))
//    var s = "erweitertes_ADJA"
//    var s = Array("erweitertes_ADJA","zusammenfassung_NN")
//    var ss: String = s.map(x=>x.split("_").head).mkString(" ")
//    println(ss)
    preprocessedSourceClasses

  }
}