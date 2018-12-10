
//import com.google.cloud.translate.Translate.TranslateOption
//import com.google.cloud.translate.TranslateOptions
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
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

  def main(args: Array[String]) {
    val startTimeMillis = System.currentTimeMillis()
//    var inputSource = "/home/shimaa/CL_Enrichment/src/main/resources/SemSur(Source).nt"
//    var inputTarget = "/home/shimaa/CL_Enrichment/src/main/resources/SemSur(Target).nt"
//    var inputTranslations = "/home/shimaa/CL_Enrichment/src/main/resources/Translation.nt"
//    var output = "/home/shimaa/CL_Enrichment/src/main/resources/Output.nt"
    //############################# Inputs for Evaluation #######################
    var inputSource = "/home/shimaa/CLOE4SC/src/main/resources/EvaluationDataset/German/conference-de-classes_updated.nt"
//var inputSource = "/home/shimaa/CLOE4SC/src/main/resources/EvaluationDataset/German/confOf-de-classes_updated.nt"
//var inputSource = "/home/shimaa/CLOE4SC/src/main/resources/EvaluationDataset/German/sigkdd-de-classes_updated.nt"
//      var inputTarget = "/home/shimaa/CLOE4SC/src/main/resources/EvaluationDataset/English/cmt-en-classes_updated.nt"
//    var inputTarget = "/home/shimaa/CLOE4SC/src/main/resources/EvaluationDataset/English/ekaw-en-classes_updated.nt"
//    var inputTarget = "/home/shimaa/CLOE4SC/src/main/resources/EvaluationDataset/English/edas-en-classes_updated.nt"
    var inputTarget = "/home/shimaa/CLOE4SC/src/main/resources/CaseStudy/SEO_classes.nt"
var pre = "/home/shimaa/CLOE4SC/src/main/resources/EvaluationDataset/German/conference-de-classes_updated_preprocessed.nt"

    //##########################################################################
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession1 = SparkSession.builder
//    .appName(subject"Triple reader example  $inputSource")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
    val lang1: Lang = Lang.NTRIPLES
    val sourceOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputSource)
    val targetOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputTarget)
    val preSOntology= sparkSession1.rdf(lang1)(pre)
//    val translations: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputTranslations)
//    val Outputs: RDD[graph.Triple] = sparkSession1.rdf(lang1)(output)
    val runTime = Runtime.getRuntime

    val ontStat = new OntologyStatistics()
    println("################### Statistics of the Source Ontology ###########################")
    ontStat.GetStatistics(sourceOntology)
      println("################### Statistics of the Target Ontology ###########################")
      ontStat.GetStatistics(targetOntology)

//    val p = new PreProcessing()
//    var preProcessedTargetOntology: RDD[(String, String, String)] = p.RecreateSourceGermanOntologyWithClassLabels(targetOntology)
//    println("############################## Preprocessed Target Ontology ##############################")
//    preProcessedTargetOntology.foreach(println(_))
//
//    var preProcessedSourceOntology: RDD[(String, String, String)] = p.RecreateSourceGermanOntologyWithClassLabels(sourceOntology)
//    println("############################## Preprocessed Source Ontology ##############################")
//    preProcessedSourceOntology.foreach(println(_))

    //    println("======================================")
//    println("|        Target Ontology       |")
//    println("======================================")
//    targetOntology.take(5).foreach(println(_))
//    println("Number of triples in target ontologyTriples = "+targetOntology.count())
//        var subject: RDD[Node] = sourceOntology.getSubjects.distinct()
//        println("First five subjects are:")
//        subject.take(5).foreach(println(_))
//
//    println("First five predicates with URIs:")
//    val targetPredicatesWithURIs = sourceOntology.getPredicates().distinct()
//    targetPredicatesWithURIs.take(5).foreach(println(_))

//    println("First five predicates without URIs:")
    val targetPredicatesWithoutURIs = targetOntology.map(_.getPredicate.getLocalName).distinct()
//    targetPredicatesWithoutURIs.take(5).foreach(println(_))

//    println("======================================")
//    println("|        Source Ontology       |")
//    println("======================================")
//    sourceOntology.take(5).foreach(println(_))
//    println("Number of triples in source ontologyTriples = "+sourceOntology.count())
//    var subject: RDD[Node] = sourceOntology.getSubjects.distinct()
//    println("First five subjects are:")
//    subject.take(5).foreach(println(_))

//    println("First five predicates with URIs:")
    val sourcePredicatesWithURIs = sourceOntology.getPredicates().distinct()
//    sourcePredicatesWithURIs.take(5).foreach(println(_))



    //############# PreProcessing #################
    println("########################## PreProcessing ##############################")
    val p = new PreProcessing()

    var preProcessedSourceOntology: RDD[(String, String, String)] = p.RecreateTargetOntologyWithClassLabels(preSOntology).cache()
    println("############################## Mapped Source Ontology ##############################"+ preProcessedSourceOntology.count())
    preProcessedSourceOntology.take(5).foreach(println(_))

    var preProcessedTargetOntology: RDD[(String, String, String)] = p.RecreateTargetOntologyWithClassLabels(targetOntology).cache() // should appliad if the classes with codes and labels
//    println("############################## Mapped Target Ontology ##############################" + preProcessedTargetOntology.count())
//    preProcessedTargetOntology.take(5).foreach(println(_))
//    println("String with tags" + p.posTagForString("regulärer Beitrag"))


    //    var preprocessedSourceClasses: RDD[(String, String)] = classes.map(c=>(c,p.stringPreProcessing(c)))//.cache()
    //    var preprocessedTargetClasses: RDD[String] = targetClasses.map(c=>p.stringPreProcessing(c))//.cache()
    //    println("Processed source classes are : ")
    //    preprocessedSourceClasses.foreach(println(_))
    //    println("Processed target classes are : ")
    //    preprocessedTargetClasses.foreach(println(_))

//    println("======================================")
//    println("|        Predefined Properties       |")
//    println("======================================")
//    println("All available predicates in the source ontology:")
    val sourcePredicatesWithoutURIs = sourceOntology.map(_.getPredicate.getLocalName).distinct()
//    sourcePredicatesWithoutURIs.foreach(println(_))
//    println("Please enter the properties without URIs separated by ',' (maximum two properties):")
//    val line=scala.io.StdIn.readLine()
//    val predefinedProperty: Array[String] = line.split(',')
    val predefinedProperty: Array[String] = Array("subClassOf", "disjointWith", "type")//line.split(',')
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


     //####################### BabelNet #####################################

      var targetClassesWithoutURIs: RDD[String] = targetOntology.map(y=>p.stringPreProcessing(y.getSubject.getLocalName)).distinct()//for ekaw-en and edas ontologies
//      var targetClassesWithoutURIs: RDD[String] = targetOntology.filter(x=>x.getPredicate.getLocalName == "label").map(y=>y.getObject.getLiteral.getLexicalForm.split("@").head)//for cmt-en ontology
    println("All classes in the target ontology Triples:" + targetClassesWithoutURIs.count())
    targetClassesWithoutURIs.foreach(println(_))
      var sourceClassesWithoutURIs = sourceOntology.filter(x=>x.getPredicate.getLocalName == "label").map(y=>y.getObject.getLiteral.getLexicalForm.split("@").head).distinct().collect()
      println("All classes in the source ontology Triples:" + sourceClassesWithoutURIs.size)
      sourceClassesWithoutURIs.foreach(println(_))
//    var sourceClassesWithoutURIs: Array[String] = subOntology.map(x=>x._1).union(subOntology.map(y=>y._3)).distinct().collect()
    var preprocessedSourceClasses: RDD[String] = sparkSession1.sparkContext.parallelize(p.posTag(sourceClassesWithoutURIs)).filter(x=>x.isEmpty == false)
//    var preprocessedSourceClassesWithOneWord: RDD[String] = preprocessedSourceClasses.filter(x=>x.split(" ").length == 1).distinct()
      println("All source classes after preprocessing "+preprocessedSourceClasses.count())
      preprocessedSourceClasses.foreach(println(_))
//    println("All source classes with one word "+preprocessedSourceClassesWithOneWord.count())
//    preprocessedSourceClassesWithOneWord.foreach(println(_))
//    var preprocessedSourceClassesWithMoreThanOneWord: RDD[String] = preprocessedSourceClasses.subtract(preprocessedSourceClassesWithOneWord).distinct()
//    println("################# A ###############################")
//      var A = preprocessedSourceClasses.sortBy(_.head)
//    A.foreach(println(_))
//    var B = sparkSession1.sparkContext.parallelize(sourceClassesWithoutURIs).sortBy(_.head)
//    println("################# B ###############################")
//    B.foreach(println(_))


//    println("All source classes with more than one word "+preprocessedSourceClassesWithMoreThanOneWord.count())
//    preprocessedSourceClassesWithMoreThanOneWord.foreach(println(_))
//    println("All classes in the sub-source ontologyTriples:")
//    println("Number of classes is "+sourceClassesWithoutURIs.length)
//    sourceClassesWithoutURIs.foreach(println(_))

//    var tt = translations.map(x=>(x.getSubject.getLocalName,x.getObject.getLocalName)).zipWithIndex().collect().toMap
//    var dd: Broadcast[Map[(String, String), Long]] = sparkSession1.sparkContext.broadcast(tt)
//
    var t = targetClassesWithoutURIs.zipWithIndex().collect().toMap
    var dc: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(t)
    val trans = new Translator(dc)
    var sourceClassesWithAllAvailableTranslations: RDD[(String, List[String])] = trans.Translate(preprocessedSourceClasses).distinct()
//    println("All source with all translations "+sourceClassesWithAllAvailableTranslations.count())
//    sourceClassesWithAllAvailableTranslations.foreach(println(_))


//    var soureClassesWithListOfTranslations: RDD[(String, String)] = sourceClassesWithAllAvailableTranslations.map{case x => if (x._2.length>1) (x._1,trans.GetBestTranslation(x._2)) else (x._1,x._2.head)}
//var soureClassesWithListOfTranslations: RDD[(String, String)] = sourceClassesWithAllAvailableTranslations.map{case x => if (x._2.length>1) (x._1,x._2.head) else (x._1,x._2.head)}
//var soureClassesWithListOfTranslations: RDD[(String, List[String], List[String])] = sourceClassesWithAllAvailableTranslations.map(x => (x._1,x._2,trans.GetBestTranslation(x._2)))
    var soureClassesWithListOfTranslations: RDD[(String, List[String], List[String])] = sourceClassesWithAllAvailableTranslations.map(x => (x._1,x._2,trans.GetBestTranslation(x._2)))//.filter(y=>y._3.length == 1)

    println("All source with best translation ")
    soureClassesWithListOfTranslations.take(70).foreach(println(_))
//
//    val bb = new BabelNet(dc)
//    var releventSourceTranslations: RDD[(String, List[String])] = preprocessedSourceClassesWithOneWord.map(s=>(s,bb.GetSynonyms(s).toList)).persist()
//    var releventSourceTranslationsWithBestSynonyms: RDD[(String, String)] = releventSourceTranslations.map(x=>(x._1,bb.BestSynonym(x._1,x._2)))
//    var sourceClassesWithTranslatedSynonyms: RDD[(String, List[String])] = preprocessedSourceClasses.map(s=>(s,bb.GetSynonyms(s).toList)).persist()
//    var sourceClassesWithBestSynonym: RDD[(String, String)] = sourceClassesWithTranslatedSynonyms.map(x=>(x._1,bb.BestSynonym(x._1,x._2))).persist()
//
//      ////
////////     var sourceClassesWithTranslatedSynonyms: RDD[(String, String)] = preprocessedSourceClasses.map(s=>(s._1,p.ToCamel(bb.GetSynonyms(s._2)))).cache()//.union(translations.map(x=>(x.getSubject.getLocalName,x.getObject.getLocalName))).distinct().cache()
//////
//    println("Source classes (one word) with synonyms")
//    releventSourceTranslations.take(24).foreach(println(_))
//    println("Source classes (one word) with best synonym")
//    releventSourceTranslationsWithBestSynonyms.take(24).foreach(println(_))
//    println("Source classes with synonyms")
//      sourceClassesWithTranslatedSynonyms.take(50).foreach(println(_))
//      println("Source classes after translation with best synonym match")
//      sourceClassesWithBestSynonym.take(50).foreach(println(_))
//
//    var relevantTranslations = sourceClassesWithTranslatedSynonyms.filter(x=>x._1 != x._2)
//    println("Relevant translations are: "+relevantTranslations.count())
//    relevantTranslations.foreach(println(_))

    println("####################### Recreating the source ontologyTriples #####################################")
            //    var sourceOntologyWithoutURI: RDD[(String, String, String)] = sourceOntology.map{case(x)=> if (x.getObject.isLiteral)(x.getSubject.getLocalName,x.getPredicate.getLocalName,x.getObject.getLiteral.toString)else (x.getSubject.getLocalName,x.getPredicate.getLocalName,x.getObject.getLocalName)}
      var sourceClassesWithBestTranslations: RDD[(String, String)] = soureClassesWithListOfTranslations.map(x=>(x._1.toLowerCase,x._3.head)).cache()
//    sourceClassesWithBestTranslations.take(10).foreach(println(_))
//    preProcessedSourceOntology.take(10).foreach(println(_))
//    var s = preProcessedSourceOntology.keyBy(_._1).join(sourceClassesWithBestTranslations.keyBy(_._1))
//    println("Source Ontology after translating the subject class")
//    s.take(10).foreach(println(_))
          val sor = new SourceOntologyReconstruction()
          var translatedSourceOntology = sor.ReconstructOntology(preProcessedSourceOntology,sourceClassesWithBestTranslations).cache()
    //    println("Source Ontology after translating subject and object classes")
                       //    translatedSourceOntology.foreach(println(_))

                    //############# ExactMatching #################
       println("Matching Two Ontologies")
       var targetOntologyWithoutURI: RDD[(String, String, String)] = targetOntology.map{case(x)=> if (x.getObject.isLiteral)(p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLiteral.toString))else (p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLocalName))}//.filter(x=>x._2 != "type" || x._2 != "comment")
    println("Target ontology without URIs")
    targetOntologyWithoutURI.foreach(println(_))
     val m = new MatchingTwoOntologies()
     var enrichedTriples = m.Match(translatedSourceOntology,targetOntologyWithoutURI)
   println("|       source triples needed for enrichment        |")
   enrichedTriples.foreach(println(_))
    var s = enrichedTriples.keyBy(_._1).leftOuterJoin(sourceClassesWithBestTranslations.map(x=>x._2).zipWithIndex().keyBy(_._1))
    println(" //############# Testing #################")
    s.take(5).foreach(println(_))

    /*                      //####################### Translation #####################################
                         println("########## Using machine translation #############")

                     //    var outputStrings: RDD[(String, String, String)] = Outputs.map(x=>(x.getSubject.getLocalName, x.getPredicate.getLocalName,x.getObject.getLocalName))
                         val mt = new ManualTranslation()
                     //    var translatedTriples: RDD[(String, String, String)] = mt.Translate(outputStrings,translations)
                         var translatedTriples = mt.Translate(enrichedTriples,translations)
                         translatedTriples.foreach(println(_))

                         //####################### Testing #####################################
                     //    println("####################### Testing #####################################")
                     //    var newTerm = "survey paper"
                     //
                     //    var newTerm1 = newTerm.split(" ").last
                     //    var newTerm2 = newTerm.split(" ").head
                     //    println("newTerm1: "+newTerm1)
                     //    println("newTerm2: "+newTerm2)
                     //


                     //    println(p.ToCamel("book chapter"))
                     //    import com.gtranslate.Translator
                     //    val translate = Translator.getInstance
                     //    val text = translate.translate("Hello", Language.ENGLISH, Language.ROMANIAN)
                     //    System.out.println(text)

                     //    var s: String = targetOntology.filterSubjects(_.isURI()).collect.mkString("\n")
                     //    var s: String = targetOntology.filterSubjects(_.hasURI("http://purl.org/semsur/Article")).collect.mkString("\n")

                         //    s.foreach(println(_))
                     //    println(s)
                     //    var sourceSubjectsURIs: RDD[(String, String)] = sourceOntology.map(x=>(x.getSubject.getURI,x.getSubject.getLocalName)).distinct()
                     //    println("Subject URIs")
                     //    sourceSubjectsURIs.foreach(println(_))
                     //    var sourceObjectsURIs = sourceOntology.filter(x=>x.getObject.isURI).map(x=> (x.getObject.getURI,x.getObject.getLocalName)).distinct()
                     //    println("Object URIs")
                     //    sourceObjectsURIs.foreach(println(_))
                     //    var sourcePredicateURIs: RDD[(String, String)] = sourceOntology.map(x=>(x.getPredicate.getURI,x.getPredicate.getLocalName)).distinct()
                     ////    sourcePredicateURIs.foreach(println(_))

                     //    val sourceClassesWithTranslatedSynonyms: RDD[(String, String)] = sparkSession1.sparkContext.parallelize(List(
                     //      ("Artikel", "Article"),
                     //      ("BuchKapitel", "BookChapter"),
                     //      ("Publikation", "Publication"))).union(translations.map(x=>(x.getSubject.getLocalName,x.getObject.getLocalName)))

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

                         //####################### Enriching #####################################
                         println("####################### Enriching #####################################")
                         val e = new Enriching()
                         var enrichedOntology = e.enrichTargetOntology(triplesToBeAddedToTarget,targetOntology)
                         enrichedOntology.foreach(println(_))

                     */
    var sim = new GetSimilarity()
//def MeanSimilarity(s1: String, s2: String): Double={
//    val cos = new Cosine(2)
//    var cosSim = cos.similarity(s1, s2)
//    System.out.println("Cosine similarity is "+cosSim)
//    import info.debatty.java.stringsimilarity.JaroWinkler
//    val jw = new JaroWinkler
//    var jarSim = jw.similarity(s1, s2)
//
//    val l = new NormalizedLevenshtein()
//    var levenshteinSim = l.distance(s1, s2)
//    System.out.println("Normalized Levenshtein similarity is "+levenshteinSim)
//
//    val j = new Jaccard(3)
//    var jaccardSim = j.similarity(s1, s2)
//    System.out.println("Jaccard similarity is "+jaccardSim)
//
//    import info.debatty.java.stringsimilarity.NGram
//    val trigram = new NGram(3)
//    var trigramSim = trigram.distance(s1, s2)
//    System.out.println("trigram similarity is "+trigram.distance(s1, s2))
//
//    import info.debatty.java.stringsimilarity.OptimalStringAlignment
//    System.out.println("\nOptimal String Alignment")
//    val osa = new OptimalStringAlignment
//
//    //Will produce 3.0
//    System.out.println(osa.distance(s1, s2))
//
//    var meanSim = (cosSim + jaccardSim+levenshteinSim+jarSim+trigramSim)/5
//    meanSim
//
//}
      var meanSim1 = sim.MeanSimilarity("call paper", "call papers")
    println("similarity = "+meanSim1)
//      var meanSim2 = sim.MeanSimilarity("regular author", "author not reviewer")
//      println("similarity = "+meanSim2)
//      println(p.stringPreProcessing2("co-chair"))
//bb.BabelNetSynSetForTest("präsentation")

//
//    import de.danielnaber.jwordsplitter.GermanWordSplitter
//    val splitter = new GermanWordSplitter(true)
//    val parts = splitter.splitWord("koferenzdokument")
//    System.out.println(parts)For
//
//    var s = "primärer author eines beitrags"
//    println(s.split(" ").last)

//    import edu.ntu.datamining.StringMetric

//    var st = new StringsMetrics()
//
//    StringMetric sm = StringMetric.getInstance(StringMetric.LEVENSHTEIN_DISTANCE);
//
//    System.out.println(sm.similarity(com1, com2));
//      import edu.stanford.nlp.tagger.maxent.MaxentTagger
//
//      var tagger = new MaxentTagger("/home/shimaa/CL_Enrichment/resources/taggers/german-fast.tagger")
//      var tags: String = tagger.tagString("mitverfasser eines beitrags")
//      println(tags)

//      import scala.io.Source
//    val src = Source.fromFile("/home/shimaa/CL_Enrichment/src/main/resources/EvaluationDataset/conference-de-translations.csv")
//    val relevantTranslations: List[List[String]] = src.getLines().toList.map(_.split(",").toList)
//    relevantTranslations.foreach(println(_))



    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("runtime = "+durationSeconds+ " seconds")

    sparkSession1.stop
  }
}
