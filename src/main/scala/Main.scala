
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
* Created by Shimaa 15.oct.2018
* */

object Main {

  def main(args: Array[String]): Unit = {
    //    val sparkConf = new SparkConf().setMaster("spark://172.18.160.16:3077")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession1 = SparkSession.builder
//      .master("spark://172.18.160.16:3090")
                .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val startTimeMillis = System.currentTimeMillis()
    //############################# Inputs for Evaluation #######################
    //val inputSource = "src/main/resources/EvaluationDataset/German/conference-de-classes_updated.nt"
    val inputSource = args(0)
    //var inputSource = "src/main/resources/EvaluationDataset/German/confOf-de-classes_updated.nt"
    //var inputSource = "src/main/resources/EvaluationDataset/German/sigkdd-de-classes_updated.nt"

    //var inputTarget = "src/main/resources/EvaluationDataset/English/cmt-en-classes_updated.nt"
    //var inputTarget = "src/main/resources/EvaluationDataset/English/ekaw-en-classes_updated.nt"
    //var inputTarget = "src/main/resources/EvaluationDataset/English/edas-en-classes_updated.nt"
    //val inputTarget = "src/main/resources/CaseStudy/SEO_classes.nt"
    val inputTarget = args(1)

    //val pre = "src/main/resources/EvaluationDataset/German/conference-de-classes_updated_preprocessed.nt"
    val pre = args(2)

    val lang1: Lang = Lang.NTRIPLES
    val sourceOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputSource)
    val targetOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputTarget)
    val preSOntology= sparkSession1.rdf(lang1)(pre)
    val runTime = Runtime.getRuntime

    val ontStat = new OntologyStatistics()
    println("################### Statistics of the Source Ontology ###########################")
    ontStat.GetStatistics(sourceOntology)
    println("################### Statistics of the Target Ontology ###########################")
    ontStat.GetStatistics(targetOntology)

    println("########################## PreProcessing ##############################")
    val p = new PreProcessing()
    val preProcessedSourceOntology: RDD[(String, String, String)] = p.RecreateOntologyWithClassLabels(preSOntology).cache()
//    println("############################## Mapped Source Ontology ##############################"+ preProcessedSourceOntology.count())
//    preProcessedSourceOntology.foreach(println(_))

    var preProcessedTargetOntology: RDD[(String, String, String)] = p.RecreateOntologyWithClassLabels(targetOntology).cache() // should applied if the classes with codes and labels
    //println("############################## Mapped Target Ontology ##############################" + preProcessedTargetOntology.count())
    //preProcessedTargetOntology.take(5).foreach(println(_))

    println("======================================")
    println("|        Predefined Properties       |")
    println("======================================")
    println("All available predicates in the source ontology:")
    val sourcePredicatesWithoutURIs = sourceOntology.map(_.getPredicate.getLocalName).distinct()
    sourcePredicatesWithoutURIs.foreach(println(_))
    println("Please enter the properties without URIs separated by ',':")
//    val line=scala.io.StdIn.readLine()
//    val predefinedProperty: Array[String] = line.split(',')
    val predefinedProperty: Array[String] = Array("subClassOf", "type")//line.split(',')
//    predefinedProperty.foreach(println(_))
//    println(predefinedProperty.take(predefinedProperty.length).apply(0) +" and " +predefinedProperty.take(predefinedProperty.length).apply(1))
//    val uPredicate1 = predefinedProperty.take(predefinedProperty.length).apply(0)
//    val uPredicate2 = predefinedProperty.take(predefinedProperty.length).apply(1)

    //############# Subgraph Extraction #################
    val subGrapgh = new SubgraphExtraction()
    val subOntology = subGrapgh.extract(preProcessedSourceOntology, predefinedProperty)
//    println("############# Sub-source Ontology ################# "+subOntology.count())
//    subOntology.foreach(println(_))
//    var sourceClasses: RDD[Node] = subOntology.getSubjects.union(subOntology.getObjects().filter(x=>x.isURI)).distinct()
//    println("All classes with URIs in the source ontology Triples:")
//    sourceClasses.foreach(println(_))
//    println("All classes in the source ontologyTriples:")
//    var classes: RDD[String] = sourceClasses.map(_.getLocalName).distinct()
//  classes.foreach(println(_))


//    var targetClasses: RDD[String] = targetOntology.getSubjects.map(_.getLocalName).distinct()
//    println("All classes in the target ontologyTriples:")
//    targetClasses.foreach(println(_))

    //####################### Translation #####################################
      var targetClassesWithoutURIs: RDD[String] = targetOntology.map(y=>p.stringPreProcessing(y.getSubject.getLocalName)).distinct()//for ekaw-en, edas and SEO ontologies
//    var targetClassesWithoutURIs: RDD[String] = targetOntology.filter(x=>x.getPredicate.getLocalName == "label").map(y=>y.getObject.getLiteral.getLexicalForm.split("@").head)//for cmt-en, confOf-de and sigkdd-de ontologies
//    println("All classes in the target ontology Triples:" + targetClassesWithoutURIs.count())
//    targetClassesWithoutURIs.foreach(println(_))
      var sourceClassesWithoutURIs = sourceOntology.filter(x=>x.getPredicate.getLocalName == "label").map(y=>y.getObject.getLiteral.getLexicalForm.split("@").head).distinct().collect()
//      println("All classes in the source ontology Triples:" + sourceClassesWithoutURIs.size)
//      sourceClassesWithoutURIs.foreach(println(_))
//    var sourceClassesWithoutURIs: Array[String] = subOntology.map(x=>x._1).union(subOntology.map(y=>y._3)).distinct().collect()
    var germanTagger: MaxentTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/german/german-fast.tagger")
    var preprocessedSourceClasses: RDD[String] = sparkSession1.sparkContext.parallelize(p.posTag(sourceClassesWithoutURIs,germanTagger)).filter(x=>x.isEmpty == false).cache()
//    var preprocessedSourceClassesWithOneWord: RDD[String] = preprocessedSourceClasses.filter(x=>x.split(" ").length == 1).distinct()
//      println("All source classes after preprocessing "+preprocessedSourceClasses.count())
//      preprocessedSourceClasses.foreach(println(_))

      //Read many-to-one translation from csv file
//    val src = Source.fromFile("src/main/resources/EvaluationDataset/Translations/Translations-conference-de_new.csv")
    //  val src = Source.fromFile("src/main/resources/EvaluationDataset/Translations/Translations-confOf-de.csv")
    //  val src = Source.fromFile("src/main/resources/EvaluationDataset/Translations/Translations-sigkdd-de.csv")
    val relevantTranslations: RDD[(String, List[String])] = sparkSession1.sparkContext.textFile(args(3)).map(_.split(",").toList).map(x=>(x.head, x.tail))
//      relevantTranslations.foreach(println(_))
//      println("Translation")

    val t = targetClassesWithoutURIs.zipWithIndex().collect().toMap
    val dc: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(t)
    val trans = new Translator(dc)
    val sourceClassesWithAllAvailableTranslations: RDD[(String, List[String])] = trans.Translate(preprocessedSourceClasses, relevantTranslations).distinct()
//    println("All source with all translations "+sourceClassesWithAllAvailableTranslations.count())
//    sourceClassesWithAllAvailableTranslations.foreach(println(_))

    var soureClassesWithListOfBestTranslations: RDD[(String, List[String], List[String])] = sourceClassesWithAllAvailableTranslations.map(x => (x._1,x._2,trans.GetBestTranslation(x._2))).cache()//.filter(y=>y._3.length == 1)

//    println("All sources with list of best translations ")
//    soureClassesWithListOfBestTranslations.take(70).foreach(println(_))


    //var sourceOntologyWithoutURI: RDD[(String, String, String)] = sourceOntology.map{case(x)=> if (x.getObject.isLiteral)(x.getSubject.getLocalName,x.getPredicate.getLocalName,x.getObject.getLiteral.toString)else (x.getSubject.getLocalName,x.getPredicate.getLocalName,x.getObject.getLocalName)}
    var soureClassesWithBestTranslation: RDD[(String, String, String)] = soureClassesWithListOfBestTranslations.map(x=> (x._1,x._2.head,x._3.head))
//    println("All sources classes with best translation ")
//    soureClassesWithBestTranslation.take(70).foreach(println(_))
//    soureClassesWithBestTranslation.saveAsTextFile("src/main/resources/EvaluationDataset/German/firstOutput.txt")
//    soureClassesWithBestTranslation.saveAsTextFile("src/main/resources/EvaluationDataset/German/ConferenceTranslations_W_R_T_SEO.txt")

    /*Experts should validate the translations in the output files*/
//    val validSourceTranslationsByExperts: RDD[(String, String)] = sparkSession1.sparkContext.textFile("src/main/resources/EvaluationDataset/Translations/Translations-conference-de_Translations_W.R.T.cmt.en.csv").map(x=>x.split(",")).map(y=>(y.head.toLowerCase,y.last.toLowerCase))
val validSourceTranslationsByExperts: RDD[(String, String)] = sparkSession1.sparkContext.textFile(args(4)).map(x=>x.split(",")).map(y=>(y.head.toLowerCase,y.last.toLowerCase))
    println("Validated translated source classes W.R.T SEO: ")
    validSourceTranslationsByExperts.take(70).foreach(println(_))

    println("####################### Recreating the source ontologyTriples #####################################")
    val sor = new SourceOntologyReconstruction()
    var translatedSourceOntology = sor.ReconstructOntology(preProcessedSourceOntology,validSourceTranslationsByExperts).filter(x=>x._2 != "disjointWith").cache()
//    println("Source Ontology after translating subject and object classes "+ translatedSourceOntology.count())
//    translatedSourceOntology.foreach(println(_))

    //############# ExactMatching #################
    println("####################### Matching Two Ontologies #######################")
    var targetOntologyWithoutURI: RDD[(String, String, String)] = targetOntology.map{case(x)=> if (x.getObject.isLiteral)(p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLiteral.toString))else (p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLocalName))}//.filter(x=>x._2 != "type" || x._2 != "comment")
//    println("Target ontology without URIs")
//    targetOntologyWithoutURI.foreach(println(_))
    val m = new MatchingTwoOntologies()
    var tripelsForEnrichment: RDD[(String, String, String, Char)] = m.Match(translatedSourceOntology,targetOntologyWithoutURI, targetClassesWithoutURIs).cache()
    println("####################### source triples needed for enrichment #######################")
    println(tripelsForEnrichment.count()+ " triples. Triples with flag 'E' are needed to enrich the target ontology. Triples with flag 'A' are new triples will be added to the target ontology.")
    tripelsForEnrichment.foreach(println(_))

    /*

//    println("All translations")
//    sourceClassesWithTranslatedSynonyms.foreach(println(_))
 var recordedTranslations: RDD[(String, String)] = sourceClassesWithTranslatedSynonyms.subtract(sourceClassesWithTranslatedSynonyms.filter(x=>x._1 == x._2)).union(translations.map(x=>(x.getSubject.getLocalName,x.getObject.getLocalName)))
//    recordedTranslations.cache()
//    recordedTranslations.foreach(println(_))
 println("Triples with URIs to be added to the target ontologyTriples")
 val r = new RetrieveURIs()
   var triplesToBeAddedToTarget = r.getTripleURIs(sourceOntology,recordedTranslations,translatedTriples)
//    var triplesToBeAddedToTarget = r.getTripleURIs(sourceOntology,sourceClassesWithTranslatedSynonyms,translatedTriples)
//    r.getTripleURIs(sourceOntology,sourceClassesWithTranslatedSynonyms,translatedTriples)

 triplesToBeAddedToTarget.foreach(println(_))

                         */

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("runtime = "+durationSeconds+ " seconds")

    sparkSession1.stop
  }
}
