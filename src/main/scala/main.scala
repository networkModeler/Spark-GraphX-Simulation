////////////////////////////////////////////////////////////////////////////////////////////////
// LOAD KAGGLE DATA FROM FILE AND CREATE SPARK GRAPHX GRAPH
//
// An Ad is modeled as a bag of words:  Ad(adId, List[String])
// A Page is modeled as a bag of words and a list of ads: Page(vertexId, List[String], List[Ad])
// The words are used for similarity metrics (page-to-page, ad-to-page)
//
// Graph structure:
//	There is a vertex for each page
//	There is an edge between any two vertices that have at least N words in common
//
// Graph operations:
//	migrate:  ads on a page may move to a neighboring page
//	propagate: attractive forces spread from their origin pages to neighboring pages
//
////////////////////////////////////////////////////////////////////////////////////////////////

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._
import java.nio.file._

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for an ad
case class Page(id: VertexId, tokens: List[String], var score: Int) extends java.io.Serializable
{
  def similarity(ad: Ad) : Int =
  {
    val commonList = this.tokens.intersect(ad.tokens)
    val numCommon = commonList.length
    return numCommon
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for an ad
case class Ad(id: Long, tokens: List[String], var next: VertexId) extends java.io.Serializable
{
  def similarity(page: Page) : Int =
  {
    val commonList = this.tokens.intersect(page.tokens)
    val numCommon = commonList.length
    return numCommon
  }

  def scorePages(pageList: List[Page]) =
  {
    // Score this page with each page in the given list
    pageList.foreach( page => page.score = page.similarity(this) )

    // Greedily select the page with the highest similarity score
    //this.next = pageList.maxBy(_.score).id

    // Create a distribution normalized from 0 to 1
    val sum = pageList.map(_.score + 1.0).sum
    val distribution = pageList.map(page => (page.id, (page.score + 1.0) / sum)).toMap

    // Randomly select a page, weighted by their similarity scores
    this.next = this.sample(distribution, pageList)

    // Statistics
    //if (this.id == 440)
    if (true)
    {
      val filtered = pageList.filter(_.id == this.id)
      if (filtered.length > 0)
      {
        val page = filtered(0)
        val score = page.score
        println("id " + this.id + " score " + score)
      }
    }
  }

  def sample(distribution: Map[VertexId, Double], pageList: List[Page]): VertexId =
  {
    var accumulator = 0.0
    val randomNumber = scala.util.Random.nextDouble

    val it = distribution.iterator
    while (it.hasNext)
    {
      val (page, score) = it.next
      accumulator += score
      if (accumulator >= randomNumber)
      {
        return page
      }
    }

    // Greedily select the page with the highest similarity score
    return pageList.maxBy(_.score).id
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for graph vertex attributes
case class VertexAttributes(var pages: List[Page], var ads: List[Ad], var step: Long) extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for migration messages passed between vertices
case class MigrateMessage(vertexId: VertexId, var ads: List[Ad]) extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for replication messages passed between vertices
case class ReplicateMessage(vertexId: VertexId, var pages: List[Page]) extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class to run the simulation
class Butterflies() extends java.io.Serializable
{
  // A boolean flag to enable debug statements
  val debug = false

  // Initialize a random number generator with a known seed for reproducibility
  //val rng = new scala.util.Random(0)

  // Create a graph from a page file and an ad file
  def createGraph(): Graph[VertexAttributes, Int] =
  {
    // Just needed for textFile() method to load an RDD from a textfile
    // Cannot use the global Spark context because SparkContext cannot be serialized from master to worker
    val sc = new SparkContext

    // Parse a text file with the vertex information
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1000pages.txt")
    val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/queryid_tokensid.txt")
      //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/trivial_nodes.txt")
      .map { l =>
      val tokens = l.split("\\s+")     // split("\\s") will split on whitespace
    val id = tokens(0).trim.toLong
      val tokenList = tokens.last.split('|').toList
      (id, tokenList)
    }
    println("********** NUMBER OF PAGES: " + pages.count + " **********")


    // Parse a text file with the ad information
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1000ads.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1000descriptions.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/purchasedkeywordid_tokensid.txt")
    val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/descriptionid_tokensid.txt")
      //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/trivial_ads.txt")
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

    // Save the list of edges as a file, to be used instead of recomputing the edges every time
    idOnly.saveAsTextFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/saved_edges")

    // Create a graph from an edge list RDD
    val edgeGraph = Graph.fromEdges[Int, Int](idOnly, 1);

    // Copy into a graph with nodes that have vertexAttributes
    val attributeGraph: Graph[VertexAttributes, Int] =
      edgeGraph.mapVertices{ (id, v) => VertexAttributes(Nil, Nil, 0) }

    // Add the node information into the graph
    val nodeGraph = attributeGraph.outerJoinVertices(pages) {
      (vertexId, attr, pageTokenList) =>
        VertexAttributes(List(Page(vertexId, pageTokenList.getOrElse(List.empty), 0)), attr.ads, attr.step)
    }

    // Add the ads to the nodes
    val adGraph = nodeGraph.outerJoinVertices(ads) {
      (vertexId, attr, adTokenList) =>
        VertexAttributes(attr.pages, List(Ad(vertexId, adTokenList.getOrElse(List.empty), 0)), attr.step)
    }

    // Display the graph for debug only
    if (debug)
    {
      println("********** GRAPH **********")
      printVertices(adGraph)
    }

    // return the generated graph
    return adGraph
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // REPLICATION
  //
  // Replicate each page's bag of words onto their adjacent neighbors
  //
  // Required because GraphX is edge-oriented, whereas this simulation is vertex-oriented
  // In order for a source vertex's ads to decide which destination vertex to migrate to,
  // we need the destination vertice's attributes replicated in the source node.
  // With this replication, we can perform the migration calculations in the source node's vprog.
  //
  // Replicating the neighboring attributes is inefficient in terms of storage, but should be
  // more efficient in terms of network traffic since the migration calculations are self contained.
  ////////////////////////////////////////////////////////////////////////////////////////////////

  // Replicate each page's bag of words onto their adjacent neighbors
  // Use the Pregel API of GraphX because it manages the caching of intermediate data better
  def replicate(graph: Graph[VertexAttributes, Int]) =
  {
    println("********** REPLICATION BEGINS  **********")

    graph.pregel[ReplicateMessage](ReplicateMessage(0L, List.empty))(replicationVprog, replicationSendMsg, replicationMergeMsg)

    println("********** REPLICATION ENDS  **********")
  }

  // Passed into Pregel, executed at the beginning of each super-step
  // Imports whatever ads were sent by neighbors
  def replicationVprog(id: VertexId, attributes: VertexAttributes, arrivals: ReplicateMessage) =
  {
    // Increment which Pregel super-step we're in
    //attributes.step += 1;

    if (debug)
    {
      println("********** replication vprog, node:" + id + ", step:" + attributes.step + " **********")
      println("pageList before")
      attributes.pages.foreach(page => print(" " + page.id))
      println()
      println("number of neighbors:" + arrivals.pages.length)
    }

    // Concatenate the neighbor's bag of words into the existing pages
    attributes.pages = attributes.pages ++ arrivals.pages  // ++ is the concatenate operator for Scala container

    if (debug)
    {
      println("pageList after")
      attributes.pages.foreach(page => print(" " + page.id))
      println()
    }

    // Return the modified attributes
    attributes
  }

  // Passed into Pregel, executed after the vprog method
  // Selects ads from this vertex's adList to send to a neighbor
  def replicationSendMsg(edge: EdgeTriplet[VertexAttributes, Int]): Iterator[(VertexId, ReplicateMessage)] =
  {
    /*
        println("********************* replicationSendMsg ******************************")
        println("resident pageList")
        edge.srcAttr.pages.foreach(page => print(" " + page.id))
        println()
        println("arriving pageList")
        edge.srcAttr.pages.foreach(page => print(" " + page.id))
        println()
        val intersect = edge.srcAttr.pages.intersect(edge.dstAttr.pages).length
        println("intersect:" + intersect)
    */

    // Check if we've already replicated along this edge
    // Pregel will terminate when there are no more messages to process
    edge.srcAttr.step += 1
    if (edge.srcAttr.step > 1)
    {
      return Iterator.empty
    }

    if (debug)
    {
      println("********** replication sendMsg, node:" + edge.dstId + ", step:" + edge.srcAttr.step + " **********")
      println("replicating pagesfrom " + edge.dstId + " to " + edge.srcId)
      edge.dstAttr.pages.foreach(page => print(" " + page))
      println()
    }

    // Queue the messages
    Iterator((edge.srcId, ReplicateMessage(edge.srcId, edge.dstAttr.pages)))
  }

  // Passed into Pregel, aggregates the messages from all neighbors
  def replicationMergeMsg(msg1: ReplicateMessage, msg2: ReplicateMessage): ReplicateMessage =
  {
    if (debug)
    {
      println("********** replication mergeMsg, node:" + msg1.vertexId + " **********")
      println("msg1")
      msg1.pages.foreach(page => print(" " + page.id))
      println()
      println("msg2")
      msg2.pages.foreach(page => print(" " + page.id))
      println()
    }

    // Merge the two page lists into a single list
    val combined = ReplicateMessage(msg1.vertexId, msg1.pages ++ msg2.pages)// ++ is the concatenate operator for Scala containers

    // Debug
    if (debug)
    {
      println("combined")
      combined.pages.foreach(page => print(" " + page.id))
      println()
    }

    // Return the modified msg1
    combined
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // MIGRATION
  ////////////////////////////////////////////////////////////////////////////////////////////////

  // Ad migration
  // Use the Pregel API of GraphX because it manages the caching of intermediate data better
  def migrate(graph: Graph[VertexAttributes, Int]) =
  {
    println("********** MIGRATION BEGINS  **********")

    graph.pregel[MigrateMessage](MigrateMessage(0L, List.empty))(migrationVprog, migrationSendMsg, migrationMergeMsg)

    println("********** MIGRATION ENDS  **********")
  }

  // Passed into Pregel, executed at the beginning of each super-step
  // Imports whatever ads were sent by neighbors
  def migrationVprog(id: VertexId, attributes: VertexAttributes, arrivals: MigrateMessage) =
  {
    if (debug)
    {
      println("********** migration vprog, node:" + id + " **********")
      println("adList before")
      attributes.ads.foreach(ad => print(" " + ad.id))
      println()
      println("number of arrivals:" + arrivals.ads.length)
    }

    // Select the next vertex for each ad (may remain in this vertex)
    attributes.ads.foreach( ad => ad.scorePages(attributes.pages))

    // Concatenate the new arrivals into the existing ads
    attributes.ads = attributes.ads ++ arrivals.ads  // ++ is the concatenate operator for Scala container

    if (debug)
    {
      println("adList after")
      attributes.ads.foreach(ad => print(" " + ad.id))
      println()
    }

    // Return the modified attributes
    attributes
  }

  // Passed into Pregel, executed after the vprog method
  // Selects ads from this vertex's adList to send to a neighbor
  def migrationSendMsg(edge: EdgeTriplet[VertexAttributes, Int]): Iterator[(VertexId, MigrateMessage)] =
  {
    // Hack to force Pregel to terminate after a certain number of super-steps
    // Pregel will terminate when there are no more messages to process
    edge.srcAttr.step = edge.srcAttr.step + 1
    if (edge.srcAttr.step > 10000)
    {
      return Iterator.empty
    }

    // Find ads that want to migrate
    val rng = new scala.util.Random(0)
    //val leaving = edge.srcAttr.ads.filter(_ => rng.nextInt(100) < 50)
    val currentVertex = edge.srcId
    val leaving = edge.srcAttr.ads.filter( ad => ad.next != currentVertex )

    if (debug)
    {
      println("********** migration sendMsg, node:" + edge.srcId + ", step:" + (edge.srcAttr.step) + " **********")
      println("number of ads leaving:" + leaving.length)
      leaving.foreach(ad => println("ad " + ad.id + " to vertex " + edge.dstId))
    }

    // Remove the ads from this vertex's ad list
    edge.srcAttr.ads = edge.srcAttr.ads filterNot (leaving contains)

    // Queue the messages
    if (leaving.nonEmpty)
    {
      Iterator((edge.dstId, MigrateMessage(edge.dstId, leaving)))
    }
    else
    {
      Iterator.empty
    }
  }

  // Passed into Pregel, aggregates the messages from all neighbors
  def migrationMergeMsg(msg1: MigrateMessage, msg2: MigrateMessage): MigrateMessage =
  {
    if (debug)
    {
      println("********** migration mergeMsg, node:" + msg1.vertexId + " **********")
      println("msg1")
      msg1.ads.foreach(ad => print(" " + ad.id))
      println()
      println("msg2")
      msg2.ads.foreach(ad => print(" " + ad.id))
      println()
    }

    // Merge the two ad lists into a single list
    val combined = MigrateMessage(msg1.vertexId, msg1.ads ++ msg2.ads)	// ++ is the concatenate operator for Scala containers

    // Debug
    if (debug)
    {
      println("combined")
      combined.ads.foreach(ad => print(" " + ad.id))
      println()
    }

    // Return the modified msg1
    combined
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  // For each vertex, print the attributes
  def printVertices(graph: Graph[VertexAttributes, Int]) =
  {
    for (vertex <- graph.vertices.collect)
    {
      println("vertex:" + vertex._1 + " pages:" + vertex._2.pages + " ads:" + vertex._2.ads)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  // Output the graph in GDF file format
  def outputGDF[VertexAttributes, Int](g:Graph[VertexAttributes, Int]) =
  {
    println("********** OUTPUT GDF FILE **********")

    val bigString =
      "nodedef>name VARCHAR\n" + g.vertices.map(v => v._1 + "\n").collect.mkString +
        "edgedef>node1 VARCHAR, node2 VARCHAR\n" + g.edges.map(e => e.srcId + "," + e.dstId + "\n").collect.mkString

    val fileName = "graph.gdf"
    val pw = new java.io.PrintWriter(fileName)
    pw.write(bigString)
    pw.close
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

} // end of class butterflies

// Companion class to call main
object Butterflies extends java.io.Serializable
{
  def main(args: Array[String])
  {
    println("********** SIMULATION STARTED  **********")

    // Initialize the Spark environment
    // Use the Kryo serialization because it's smaller and faster than the Java serialization
    //val conf = new SparkConf().setAppName("Butterflies")
    //conf.set("spark.serializer", "JavaSerializer")
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[Ad], classOf[List[Ad]], classOf[VertexAttributes], classOf[Butterflies]))
    //val sc = new SparkContext(conf)

    val sim = new Butterflies
    val initialGraph: Graph[VertexAttributes, Int] = sim.createGraph
    //sim.replicate(initialGraph)
    //sim.migrate(initialGraph)
    //sim.outputGDF(initialGraph)

    println("********** SIMULATION COMPLETE **********")
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////

