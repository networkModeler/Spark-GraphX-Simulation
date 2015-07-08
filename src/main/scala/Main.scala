//////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////
//
// SIMULATOR OF ADS MIGRATING ALONG A SIMILARITY GRAPH OF PAGES
//
// Model:
// 	An Ad is modeled as a bag of words:  Ad(adId, List[String])
// 	A Page is modeled as a bag of words and a list of ads: Page(vertexId, List[String], List[Ad])
// 	Ads move from page to page,  with randomness proportional to the similarity between the ad and the adjacent pages
// 	For now, the similarity metric is simply the number of words in common (page-to-page, ad-to-page)
//
// Graph structure:
//      There is a vertex for each page
//      There is an edge between any two vertices with a similarity above a certain threshold
//
// Simulator operations:
//	Create graph
//	Replicate data
//	Insert ads
//	Migrate ads
//	Output graph
//
// Implementation:
//	Implemented with Spark GraphX, Pregel API
//
////////////////////////////////////////////////////////////////////////////////////////////////

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._

// Scala object to hold the main static function
object Main extends java.io.Serializable
{
  def main(args: Array[String])
  {
    println("********** SIMULATION STARTED  **********")

    // Initialize the Spark environment
    val conf = new SparkConf().setAppName("Butterflies")

    // Kryo serialization is smaller and faster than Java serialization
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Page], classOf[Ad], classOf[List[Ad]],
                             classOf[VertexAttributes], classOf[Simulator]))

    // Tweaking for problems with network
    conf.set("spark.akka.executor.heartbeatInterval", "10000")
    conf.set("spark.akka.heartbeat.interval", "1000")
    conf.set("spark.akka.heartbeat.pauses", "6000")
    conf.set("spark.akka.failure-detector.threshold", "300")
    conf.set("spark.worker.timeout", "60")

    // Run the simulator
    val sim = new Simulator
    val initialGraph: Graph[VertexAttributes, Int] = sim.createGraph
    sim.replicate(initialGraph)
    sim.migrate(initialGraph, 100)

    // Output the graph in GDF format for Gephi visualization
    sim.outputGDF(initialGraph)

    println("********** SIMULATION COMPLETE **********")
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////


