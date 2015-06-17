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
import java.io._

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for an ad
case class Ad(ID: Long, tokens: List[String]) extends java.io.Serializable
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
case class VertexAttributes(keywords: List[String], var ads: List[Ad]) extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for messages passed between vertices
case class Message(vertexId: VertexId, var ads: List[Ad]) extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class to run the simulation
class Butterflies() extends java.io.Serializable
{
  // Create a graph from a page file and an ad file
  def createGraph(): Graph[VertexAttributes, Int] =
  {
    val sc = new SparkContext

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

    // Save the list of edges as a file, to be used instead of recomputing the edges every time
    idOnly.saveAsTextFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/saved_edges")

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
    println("********** MIGRATION BEGINS  **********")

    //graph.pregel[(VertexId, List[Ad])]((0L, List.empty))(vprog, sendMsg, mergeMsg)
    graph.pregel[Message](Message(0L, List.empty))(vprog, sendMsg, mergeMsg)

    println("********** MIGRATION ENDS  **********")
  }

  // Passed into Pregel, executed at the beginning of each super-step
  // Imports whatever ads were sent by neighbors
  //def vprog(id: VertexId, attributes: VertexAttributes, arrivals: (VertexId, List[Ad])) =
  def vprog(id: VertexId, attributes: VertexAttributes, arrivals: Message) =
  {
    // Debug
    println("********** vprog, node:" + id + " **********")
    println("adList before")
    attributes.ads.foreach(ad => print(" " + ad.ID))
    println()
    println("number of arrivals:" + arrivals.ads.length)

    // Concatenate the new arrivals into the existing ads
    attributes.ads = attributes.ads ++ arrivals.ads  // ++ is the concatenate operator for Scala container

    // Debug
    println("adList after")
    attributes.ads.foreach(ad => print(" " + ad.ID))
    println()

    // Return the modified attributes
    attributes
  }

  // Passed into Pregel, executed after the vprog method
  // Selects ads from this vertex's adList to send to a neighbor
  //def sendMsg(edge: EdgeTriplet[VertexAttributes, Int]): Iterator[(VertexId, (VertexId, List[Ad]))] =
  def sendMsg(edge: EdgeTriplet[VertexAttributes, Int]): Iterator[(VertexId, Message)] =
  {
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
      //val leaving = edge.srcAttr.ads.filter(_ => rng.nextInt(100) < 50)
      val leaving = edge.srcAttr.ads.filter(_ => rng.nextInt(100) < 100)

      // Debug
      println("********** sendMsg, node:" + edge.srcId + ", step:" + (edge.attr - 1) + " **********")
      println("number of ads leaving:" + leaving.length)
      leaving.foreach(ad => println("ad " + ad.ID + " to vertex " + edge.dstId))

      // Remove the ads from this vertex's ad list
      edge.srcAttr.ads = edge.srcAttr.ads filterNot (leaving contains)

      // Queue the messages
      if (leaving.nonEmpty)
      {
        Iterator((edge.dstId, Message(edge.dstId, leaving)))
      }
      else
      {
        Iterator.empty
      }
    }
  }

  // Passed into Pregel, aggregates the messages from all neighbors
  //def mergeMsg(msg1: (VertexId, List[Ad]), msg2: (VertexId, List[Ad])): (VertexId, List[Ad]) =
  def mergeMsg(msg1: Message, msg2: Message): Message =
  {
    // Debug
    println("********** mergeMsg, node:" + msg1.vertexId + " **********")
    println("msg1")
    msg1.ads.foreach(ad => print(" " + ad.ID))
    println()
    println("msg2")
    msg2.ads.foreach(ad => print(" " + ad.ID))
    println()

    // Merge the two ad lists into a single list
    val combined = Message(msg1.vertexId, msg1.ads ++ msg2.ads) // ++ is the concatenate operator for Scala containers

    // Debug
    println("combined")
    msg2.ads.foreach(ad => print(" " + ad.ID))
    println()

    // Return the modified msg1
    combined
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

  // Output the graph in GDF file format

  def outputGDF[VertexAttributes, Int](g:Graph[VertexAttributes, Int]) =
  {
    val bigString =
      "nodedef>name VARCHAR\n" + g.vertices.map(v => v._1 + "\n").collect.mkString +
      "edgedef>node1 VARCHAR, node2 VARCHAR\n" + g.edges.map(e => e.srcId + "," + e.dstId + "\n").collect.mkString

    new PrintWriter("graph.gdf") { write(bigString); close }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

} // end of class butterflies

// Companion class to call main
object Butterflies extends java.io.Serializable
{
  def main(args: Array[String])
  {
    // Initialize the Spark environment
    // Use the Kryo serialization because it's smaller and faster than the Java serialization
    //val conf = new SparkConf().setAppName("Butterflies")
    //conf.set("spark.serializer", "JavaSerializer")
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[Ad], classOf[List[Ad]], classOf[VertexAttributes], classOf[Butterflies]))
    //val sc = new SparkContext(conf)

    val sim = new Butterflies
    val initialGraph: Graph[VertexAttributes, Int] = sim.createGraph
    //sim.outputGDF(initialGraph)
    sim.migrate(initialGraph)
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////


