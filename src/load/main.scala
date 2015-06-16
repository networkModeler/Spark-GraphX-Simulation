////////////////////////////////////////////////////////////////////////////////////////////////
// LOAD KAGGLE DATA FROM FILE AND CREATE SPARK GRAPHX GRAPH
//
// An Ad is modeled as a bag of words:  Ad(adId, List[String])
// A Page is modeled as a bag of words and a list of ads: Page(vertexId, List[String], List[Ad])
// The words are used for similarity metrics (page-to-page, ad-to-page)
//
// Graph structure:
//      There is a vertex for each page
//      There is an edge between any two vertices that have at least N words in common
//
// Graph operations:
//      migrate:  ads on a page may move to a neighboring page
//      propagate: attractive forces spread from their origin pages to neighboring pages
//
////////////////////////////////////////////////////////////////////////////////////////////////

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for an ad
case class Ad(ID: Long, tokens: List[String]) //extends java.io.Serializable
{
  def similar(otherAd: Ad) : Int =
  {
     val commonList = this.tokens.intersect(otherAd.tokens)
     val numCommon = commonList.length
     return numCommon
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for graph vertex attributes
case class VertexAttributes(keywords: List[String], var ads: List[Ad]) //extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class to run the simulation
class Butterflies //extends java.io.Serializable
{
  // Initialize the Spark environment
  // Use the Kryo serialization because it's smaller and faster than the Java serialization
  val conf = new SparkConf().setAppName("Butterflies")
  //conf.set("spark.serializer", "JavaSerializer")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerKryoClasses(Array(classOf[Ad], classOf[List[Ad]], classOf[VertexAttributes], classOf[Butterflies]))
  val sc = new SparkContext(conf)

  // Create a graph from a page file and an ad file
  def createGraph(): Graph[VertexAttributes, Int] =
  {
    // Parse a text file with the vertex information
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1000pages.txt")
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/queryid_tokensid.txt")
    val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/trivial_nodes.txt")
      .map { l =>
        val tokens = l.split("\\s+")     // split("\\s") will split on whitespace
        val id = tokens(0).trim.toLong
        val tokenList = tokens.last.split('|').toList
        (id, tokenList)
      }
    //pages.saveAsTextFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/saved_pages")
    println("********** NUMBER OF PAGES: " + pages.count + " **********")


    // Parse a text file with the ad information
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1000ads.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/purchasedkeywordid_tokensid.txt")
    val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/trivial_ads.txt")
      .map { l =>
        val tokens = l.split("\\s+")     // split("\\s") will split on whitespace
        val id = tokens(0).trim.toLong
        val tokenList = tokens.last.split('|').toList
        (id, tokenList)
      }
    println("********** NUMBER OF ADS: " + ads.count + " **********")

    // Create the edges between similar pages
    //   Create of list of all possible pairs of pages
    //   Check if any pair shares at least one token
    //   We only need the pair id's for the edgelist
    val allPairs = pages.cartesian(pages).filter{ case (a, b) => a._1 < b._1 }
    val similarPairs = allPairs.filter{ case (page1, page2) => page1._2.intersect(page2._2).length >= 1 }
    val idOnly = similarPairs.map{ case (page1, page2) => Edge(page1._1, page2._1, 1)}
    //idOnly.saveAsTextFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/saved_edges")
    println("********** NUMBER OF EDGES: " + idOnly.count + " **********")

    // Create a graph from an edge list RDD
    val edgeGraph = Graph.fromEdges[Int, Int](idOnly, 1);

    // Copy into a graph with nodes that have vertexAttributes
    val attributeGraph: Graph[VertexAttributes, Int] =
      edgeGraph.mapVertices{ (id, v) => VertexAttributes(Nil, Nil) }

    // Add the node information into the graph
    val nodeGraph = attributeGraph.outerJoinVertices(pages) {
      case (vertexId, attr, pageTokenList) => VertexAttributes(pageTokenList.getOrElse(List.empty), attr.ads)
    }

    // Add the ads to the nodes
    val adGraph = nodeGraph.outerJoinVertices(ads) {
      (vertexId, attr, adTokenList) =>
        VertexAttributes(attr.keywords, List(Ad(vertexId, adTokenList.getOrElse(List.empty))))
    }

    // Display the graph for debug only
    println("********** GRAPH **********")
    printVertices(adGraph)

    // return the generated graph
    return adGraph
  }


  ////////////////////////////////////////////////////////////////////////////////////////////////
  // MIGRATION
  ////////////////////////////////////////////////////////////////////////////////////////////////

  // Ad migration
  // Use the Pregel API of GraphX because it manages the caching of intermediate data better
  def migrate(graph: Graph[VertexAttributes, Int]) =
  {
    // Debug
    println("********** migrate  **********")

    graph.pregel[(VertexId, List[Ad])]((0L, List.empty))(vprog, sendMsg, mergeMsg)
  }

  // Passed into Pregel, executed at the beginning of each super-step
  // Imports whatever ads were sent by neighbors
  def vprog(id: VertexId, attributes: VertexAttributes, arrivals: (VertexId, List[Ad])) =
  {
    // Debug
    println("********** vprog  **********")
    attributes.ads.foreach(println)

    // Concatenate the new arrivals into the existing ads
    attributes.ads ++ arrivals._2  // ++ is the concatenate operator for Scala container

    // Return the modified attributes
    attributes
  }

  // Passed into Pregel, executed after the vprog method
  // Selects ads from this vertex's adList to send to a neighbor
  def sendMsg(edge: EdgeTriplet[VertexAttributes, Int]): Iterator[(VertexId, (VertexId, List[Ad]))] =
  {
    // Debug
    println("********** sendMsg  **********")

    // Hack to force Pregel to terminate after a certain number of super-steps
    edge.attr = edge.attr + 1;
    if (edge.attr > 2)
    {
      Iterator.empty
    }
    else
    {
      // Find ads that want to migrate
      val rng = scala.util.Random
      val leaving = edge.srcAttr.ads.filter(_ => rng.nextInt(100) < 50)

      // Remove the ads from this vertex's ad list
      edge.srcAttr.ads = edge.srcAttr.ads filterNot (leaving contains)

      // Queue the messages
      if (leaving.nonEmpty)
      {
        Iterator((edge.dstId, (edge.srcId, leaving)))
      }
      else
      {
        Iterator.empty
      }
    }
  }

  // Passed into Pregel, aggregates the messages from all neighbors
  def mergeMsg(msg1: (VertexId, List[Ad]), msg2: (VertexId, List[Ad])): (VertexId, List[Ad]) =
  {
    // Debug
    println("********** mergeMsg  **********")

    // Merge the two ad lists into a single list
    msg1._2 ++ msg2._2  // ++ is the concatenate operator for Scala containers
    msg1
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  // For each vertex, print the attributes
  def printVertices(graph: Graph[VertexAttributes, Int]) =
  {
    for (vertex <- graph.vertices.collect)
    {
      println("vertex:" + vertex._1 + " words:" + vertex._2.keywords + " ads:" + vertex._2.ads)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

} // end of class butterflies

// Companion class to call main
object Butterflies //extends java.io.Serializable
{
  def main(args: Array[String])
  {
    val sim = new Butterflies
    val initialGraph: Graph[VertexAttributes, Int] = sim.createGraph
    sim.migrate(initialGraph)
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////

