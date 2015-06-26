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

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for an page
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
  val debug = false

  def similarity(page: Page) : Int =
  {
     val commonList = this.tokens.intersect(page.tokens)
     val numCommon = commonList.length
     return numCommon
  }

  def scorePages(pageList: List[Page], currentPage: VertexId) =
  {
    // Score this ad with each page in the given list
    pageList.foreach( page => page.score = page.similarity(this) )

    // Greedily select the page with the highest similarity score
    //this.next = pageList.maxBy(_.score).id

    // Randomly select a page, weighted by their similarity scores
    this.next = this.selectWeightedRandom(pageList, currentPage)

    // Statistics
    if (debug)
    {
      val filtered = pageList.filter(_.id == this.next)
      if (filtered.length > 0)
      {
        println("********** scorePages, node=" + currentPage + " **********")
        pageList.foreach(println)

        val page = filtered(0)
        val score = page.score
        println("migrate ad " + this.id + " from page " + currentPage + " to page " + this.next + ", score " + score)
      }
    }
  }

  // Select a page, randomly in proportion to the page's score
  def selectWeightedRandom(pageList: List[Page], currentPage: VertexId): VertexId = 
  {
    // Check if there are no neighboring vertices
    if (pageList.length == 1)
    {
      return pageList(0).id
    }

    // Create a distribution normalized from 0 to 1
    val sum = pageList.map(_.score + 1.0).sum	// + 1.0 because many scores will be zero
    val distribution = pageList.map(page => (page.id, (page.score + 1.0) / sum)).toMap

    // Try a few times in case it selects current page
    var numTries = 10
    var selectedPage = currentPage
    while ((selectedPage == currentPage) && (numTries > 0))
    {
      numTries = numTries - 1

      // Get a random number
      val randomNumber = scala.util.Random.nextDouble

      // See where the random number lands amongst the pages
      var accumulator = 0.0
      val it = distribution.iterator
      while (it.hasNext) 
      {
        val (page, score) = it.next
        accumulator += score
        if (accumulator >= randomNumber)
        {
          selectedPage = page
        }
      }

      // We land here if there are duplicates in the pageList
      // Greedily select the page with the highest similarity score
      if (selectedPage == currentPage)
      {
        selectedPage = pageList.maxBy(_.score).id
      }
    }

    return selectedPage
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for GraphX vertex attributes
case class VertexAttributes(var pages: List[Page], var ads: List[Ad], var step: Long, inDegree: Int, outDegree: Int)
  extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for migration Pregel messages passed between vertices
case class MigrateMessage(vertexId: VertexId, var ads: List[Ad]) extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for replication Pregel messages passed between vertices
case class ReplicateMessage(vertexId: VertexId, var pages: List[Page]) extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class to run the simulation
class Butterflies() extends java.io.Serializable
{
  // A boolean flag to enable debug statements
  var debug = false

  // A boolean flag to read an edgelist file rather than compute the edges
  val readEdgelistFile = true;

  // Create a graph from a page file and an ad file
  def createGraph(): Graph[VertexAttributes, Int] = 
  {
    // Just needed for textFile() method to load an RDD from a textfile
    // Cannot use the global Spark context because SparkContext cannot be serialized from master to worker
    val sc = new SparkContext

    // Parse a text file with the vertex information
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1M_nodes.txt")
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/10K_nodes.txt")
    val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/3K_nodes.txt")
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/100_nodes.txt")
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/queryid_tokensid.txt")
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/10Mpages.txt")
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1Mpages.txt")
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1000pages.txt")
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/trivial_nodes.txt")
      .map { l =>
        val tokens = l.split("\\s+")     // split("\\s") will split on whitespace
        val id = tokens(0).trim.toLong
        val tokenList = tokens.last.split('|').toList
        (id, tokenList)
      }
    println("********** NUMBER OF PAGES: " + pages.count + " **********")

    // Parse a text file with the ad information
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1M_ads.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/10K_ads.txt")
    val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/3K_ads.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/100_ads.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/descriptionid_tokensid.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/purchasedkeywordid_tokensid.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1000descriptions.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/1000ads.txt")
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/trivial_ads.txt")
      .map { l =>
        val tokens = l.split("\\s+")     // split("\\s") will split on whitespace
        val id = tokens(0).trim.toLong
        val tokenList = tokens.last.split('|').toList
        val next: VertexId = 0
        //val vertexId: VertexId = id % 1000
        val vertexId: VertexId = id
        (vertexId, Ad(id, tokenList, next))
      }
    println("********** NUMBER OF ADS: " + ads.count + " **********")

    // Check if we should simply read an edgelist file, or compute the edges from scratch
    val edgeGraph =
    if (readEdgelistFile)
    {
      // Create a graph from an edgelist file
      //GraphLoader.edgeListFile(sc, "hdfs://ip-172-31-4-59:9000/user/butterflies/data/1M_edges.txt")
      //GraphLoader.edgeListFile(sc, "hdfs://ip-172-31-4-59:9000/user/butterflies/data/10K_edges.txt")
      GraphLoader.edgeListFile(sc, "hdfs://ip-172-31-4-59:9000/user/butterflies/data/3K_edges.txt")
      //GraphLoader.edgeListFile(sc, "hdfs://ip-172-31-4-59:9000/user/butterflies/data/100_edges.txt")
      //GraphLoader.edgeListFile(sc, "hdfs://ip-172-31-4-59:9000/user/butterflies/data/1000_saved_edges.txt")
    }
    else
    {
      // Create the edges between similar pages
      //   Create of list of all possible pairs of pages
      //   Check if any pair shares at least one token
      //   We only need the pair id's for the edgelist
      val allPairs = pages.cartesian(pages).filter{ case (a, b) => a._1 < b._1 }
      val similarPairs = allPairs.filter{ case (page1, page2) => page1._2.intersect(page2._2).length >= 1 }
      val idOnly = similarPairs.map{ case (page1, page2) => Edge(page1._1, page2._1, 1)}
      println("********** NUMBER OF EDGES: " + idOnly.count + " **********")

      // Save the list of edges as a file, to be used instead of recomputing the edges every time
      //idOnly.saveAsTextFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/saved_edges")

      // Create a graph from an edge list RDD
      Graph.fromEdges[Int, Int](idOnly, 1);
    }

    // Copy into a graph with nodes that have vertexAttributes
    val attributeGraph: Graph[VertexAttributes, Int] =
      edgeGraph.mapVertices{ (id, v) => VertexAttributes(Nil, Nil, 0, 0, 0) }

    // Add the node information into the graph
    val nodeGraph = attributeGraph.outerJoinVertices(pages) {
      (vertexId, attr, pageTokenList) => 
        VertexAttributes(List(Page(vertexId, pageTokenList.getOrElse(List.empty), 0)), 
                         attr.ads, attr.step, attr.inDegree, attr.outDegree)
    }

    // Add the node degree information into the graph
    val degreeGraph = nodeGraph
    .outerJoinVertices(nodeGraph.inDegrees) 
    {
      case (id, attr, inDegree) => VertexAttributes(attr.pages, attr.ads, attr.step, inDegree.getOrElse(0), attr.outDegree)
    }
    .outerJoinVertices(nodeGraph.outDegrees) 
    {
      case (id, attr, outDegree) => 
        VertexAttributes(attr.pages, attr.ads, attr.step, attr.inDegree, outDegree.getOrElse(0))
    }

    // Add the ads to the nodes
    val adGraph = degreeGraph.outerJoinVertices(ads) 
    {
      (vertexId, attr, ad) => 
      {
        if (ad.isEmpty)
        {
          VertexAttributes(attr.pages, List.empty, attr.step, attr.inDegree, attr.outDegree)
        }
        else
        {
          VertexAttributes(attr.pages, List(Ad(ad.get.id, ad.get.tokens, ad.get.next)), attr.step, attr.inDegree, attr.outDegree)
        }
      }
    }

    // Display the graph for debug only
    if (debug)
    {
      println("********** GRAPH **********")
      //printVertices(adGraph)
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

    graph.pregel[ReplicateMessage](ReplicateMessage(0L, List.empty), maxIterations = 1)(replicationVprog, replicationSendMsg, replicationMergeMsg)

    println("********** REPLICATION ENDS  **********")
  }

  // Passed into Pregel, executed at the beginning of each super-step
  // Imports whatever ads were sent by neighbors
  def replicationVprog(id: VertexId, attributes: VertexAttributes, arrivals: ReplicateMessage) =
  {
    // Increment which Pregel super-step we're in
    attributes.step += 1;

    if (debug)
    {
      if (arrivals.pages.length > 0)
      {
        println("********** replication vprog, node:" + id + ", step:" + attributes.step + " **********")
        println("pageList before")
        attributes.pages.foreach(page => print(" " + page.id))
        println()
      }
    }

    // Concatenate the neighbor's bag of words into the existing pages
    attributes.pages = (attributes.pages ++ arrivals.pages).distinct  // ++ is the concatenate operator for Scala container

    if (debug)
    {
      if (arrivals.pages.length > 0)
      {
        println("pageList after")
        attributes.pages.foreach(page => print(" " + page.id))
        println()
        println("number of neighbors:" + arrivals.pages.length)
      }
    }

    // Return the modified attributes
    attributes
  }

  // Passed into Pregel, executed after the vprog method
  // Selects ads from this vertex's adList to send to a neighbor
  def replicationSendMsg(edge: EdgeTriplet[VertexAttributes, Int]): Iterator[(VertexId, ReplicateMessage)] =
  {
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
    //if (debug)
    if (false)
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

    //if (debug)
    if (false)
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
  def migrate(graph: Graph[VertexAttributes, Int], iterations: Int) =
  {
    println("********** MIGRATION BEGINS  **********")

    graph.pregel[MigrateMessage](MigrateMessage(-1L, List.empty), maxIterations = iterations)(migrationVprog, migrationSendMsg, migrationMergeMsg)

    println("********** MIGRATION ENDS  **********")
  }

  // Passed into Pregel, executed at the beginning of each super-step
  // Imports whatever ads were sent by neighbors
  def migrationVprog(id: VertexId, attributes: VertexAttributes, arrivals: MigrateMessage) =
  {
    // Increment which Pregel super-step we're in
    attributes.step += 1;

    //val localDebug = arrivals.ads.exists( ad => ad.id == 78)
    //if (debug || localDebug)
    if (debug)
    {
      println("********** migration vprog, node:" + id + " **********")
      println("adList before")
      attributes.ads.foreach(ad => print(" " + ad.id))
      println()
      println("number of arrivals:" + arrivals.ads.length)
      println("inDegree:" +  attributes.inDegree + " outDegree:" + attributes.outDegree)
    }

    // Concatenate the new arrivals into the existing ads
    attributes.ads = (attributes.ads ++ arrivals.ads.filter(_.id >= 0)).distinct  // ++ is the concatenate operator for Scala container

    // Select the next vertex for each ad (may remain in this vertex)
    attributes.ads.foreach( ad => ad.scorePages(attributes.pages, id))

    //if (debug || localDebug)
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
    // Find ads that want to migrate
    val currentVertex = edge.srcId
    val leaving = edge.srcAttr.ads.filter( ad => ad.next != currentVertex )

    //val localDebug = edge.srcAttr.ads.exists( ad => ad.id == 78)
    //if (debug || localDebug)
    if (debug)
    {
      println("********** migration sendMsg, node:" + edge.srcId + ", step:" + (edge.srcAttr.step) + " **********")
      println("adList before")
      edge.srcAttr.ads.foreach(ad => print(" " + ad.id))
      println()
      println("number of ads leaving:" + leaving.length)
      leaving.foreach(ad => println("ad " + ad.id + " to vertex " + edge.dstId))
    }

    // Remove the ads from this vertex's ad list
    edge.srcAttr.ads = edge.srcAttr.ads.filterNot(leaving.contains(_))

    //if (debug || localDebug)
    if (debug)
    {
      println("adList after")
      edge.srcAttr.ads.foreach(ad => print(" " + ad.id))
      println()
    }

    // Queue the messages
    if (leaving.nonEmpty)
    {
      Iterator((edge.dstId, MigrateMessage(edge.dstId, leaving)))
    }
    // Pregel only invokes sendMsg on vertices that received a message on the previous round
    // We must send a dummy message to vertices that have ads or else they'll never leave that vertex
    else if (edge.dstAttr.ads.length > 0)
    {
      Iterator((edge.dstId, MigrateMessage(-1L, List.empty)))
    }
    else
    {
      Iterator.empty
    }
  }

  // Passed into Pregel, aggregates the messages from all neighbors
  def migrationMergeMsg(msg1: MigrateMessage, msg2: MigrateMessage): MigrateMessage =
  {
    //if (debug)
    if (false)
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
    //if (debug)
    if (false)
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

    //val fileName = "hdfs://ip-172-31-4-59:9000/user/butterflies/data/graph.gdf"
    //val pw = new java.io.PrintWriter(fileName) 

    // Output the nodes
    //pw.write("nodedef>name VARCHAR, color VARCHAR\n")
    //g.vertices.map(v => pw.write(v._1 + "," + v._1%3 + "\n"))
    //val nodeRdd = g.vertices.map(v => if (v._2.pages(0).score == 0)(v._1 + ",'0,0,255'") else (v._1 + ",'255,0,0'"))
    val nodeRdd = g.vertices.map(v => if (v._1 % 2 == 0)(v._1 + ",'0,0,255'") else (v._1 + ",'255,0,0'"))
    nodeRdd.saveAsTextFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/nodes.gdf")

    // Output the edges
    //pw.write("edgedef>node1 VARCHAR, node2 VARCHAR\n")
    //g.edges.map(e => pw.write(e.srcId + "," + e.dstId + "\n"))
    val edgeRdd = g.edges.map(e => (e.srcId + "," + e.dstId))
    edgeRdd.saveAsTextFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/edges.gdf")

    //pw.close
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
    val conf = new SparkConf().setAppName("Butterflies")
    //conf.set("spark.serializer", "JavaSerializer")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Page], classOf[Ad], classOf[List[Ad]], 
                             classOf[VertexAttributes], classOf[Butterflies]))

    val sim = new Butterflies
    val initialGraph: Graph[VertexAttributes, Int] = sim.createGraph
    sim.replicate(initialGraph)
    sim.migrate(initialGraph, 100)
    sim.outputGDF(initialGraph)

    println("********** SIMULATION COMPLETE **********")
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////

