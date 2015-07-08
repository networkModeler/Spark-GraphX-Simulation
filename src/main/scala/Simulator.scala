//////////////////////////////////////////////////////////////////////////////////////////////
// 
// SIMULATOR CLASS:  driver functions for the simulation
//
// Implementation:
//	Uses the Spark GraphX Pregel API.
//	GraphX is implemented in Scala and data is predominantly immutable.
//	The Pregel API is recommended for iterative algorithms, such as this simulation,
//	because the API tries to manage the intermediate data and the in-memory cache.
//
//	A major implementation issue is that the Spark GraphX Pregel API is edge-oriented.
//	At no point in the Pregel super-step does a vertex have visibility to all its adjacent vertices.
//	Since this simulation is inherently vertex-oriented, this implementation replicates the data
//	of a vertex onto each of its adjacent vertices.  This trick makes the vertex calcuations
//	self-contained, thus making the simulation feasible on GraphX.  Obviously this increases
//	the storage requirements, but we win big by cutting down the inter-vertex communication.
//	For this simulation of a more or less randomly connected graph with no clean partitioning,
//	network traffic is more of a concern than storage.
//
//	The replication of vertex data is performed during initialization (this project assumes
//	a static graph), implemented as its own Pregel cycle.
//
////////////////////////////////////////////////////////////////////////////////////////////////

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for replication Pregel messages passed between vertices
// Sends a vertex's page data to its neighboring vertex
case class ReplicateMessage(vertexId: VertexId, var pages: List[Page]) extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for migration Pregel messages passed between vertices
// Sends a list of ads that are migrating from the source vertex to the destination vertex
case class MigrateMessage(vertexId: VertexId, var ads: List[Ad]) extends java.io.Serializable

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class to run the simulation
class Simulator() extends java.io.Serializable
{
  // A boolean flag to enable debug statements
  var debug = true

  // A boolean flag to read an edgelist file rather than compute the edges
  // Because computing the edges is expensive (N^2), only compute it once and then just reload to run the simulation
  val readEdgelistFile = true;

  ////////////////////////////////////////////////////////////////////////////////////////////////

  // Create a graph from a page file and an ad file
  def createGraph(): Graph[VertexAttributes, Int] = 
  {
    // Just needed for textFile() method to load an RDD from a textfile
    // Cannot use the global Spark context because SparkContext cannot be serialized from master to worker
    val sc = new SparkContext

    // Parse a text file with the vertex information
    //val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/queryid_tokensid.txt")
    val pages = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/3K_nodes.txt")
      .map { l =>
        val tokens = l.split("\\s+")     // split("\\s") will split on whitespace
        val id = tokens(0).trim.toLong
        val tokenList = tokens.last.split('|').toList
        (id, tokenList)
      }
    println("********** NUMBER OF PAGES: " + pages.count + " **********")

    // Check if we should simply read an edgelist file, or compute the edges from scratch
    val edgeGraph =
    if (readEdgelistFile)
    {
      // Create a graph from an edgelist file
      //GraphLoader.edgeListFile(sc, "hdfs://ip-172-31-4-59:9000/user/butterflies/data/saved_edges.txt")
      GraphLoader.edgeListFile(sc, "hdfs://ip-172-31-4-59:9000/user/butterflies/data/3K_edges.txt")
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
    //val attributeGraph: Graph[VertexAttributes, Int] =
    val attributeGraph = edgeGraph.mapVertices{ (id, v) => new VertexAttributes(Nil, Nil, 0, 0, 0) }

    // Add the node information into the graph
    val nodeGraph = attributeGraph.outerJoinVertices(pages) {
      (vertexId, attr, pageTokenList) => 
        new VertexAttributes(List(Page(vertexId, pageTokenList.getOrElse(List.empty), 0)), 
                         attr.ads, attr.step, attr.inDegree, attr.outDegree)
    }

    // Add the node degree information into the graph
    val degreeGraph = nodeGraph
    .outerJoinVertices(nodeGraph.inDegrees) 
    {
      case (id, attr, inDegree) => new VertexAttributes(attr.pages, attr.ads, attr.step, inDegree.getOrElse(0), attr.outDegree)
    }
    .outerJoinVertices(nodeGraph.outDegrees) 
    {
      case (id, attr, outDegree) => 
        new VertexAttributes(attr.pages, attr.ads, attr.step, attr.inDegree, outDegree.getOrElse(0))
    }

    // Parse a text file with the ad information
    //val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/descriptionid_tokensid.txt")
    val ads = sc.textFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/3K_ads.txt")
      .map { l =>
        val tokens = l.split("\\s+")     // split("\\s") will split on whitespace
        val id = tokens(0).trim.toLong
        val tokenList = tokens.last.split('|').toList
        val next: VertexId = 0
        val score = 0
        val vertexId: VertexId = (id + 1) % 3000	// assign ad to arbitrary page
        (vertexId, Ad(id, tokenList, next, score))
      }
    println("********** NUMBER OF ADS: " + ads.count + " **********")

    // Add the ads to the nodes
    val adGraph = degreeGraph.outerJoinVertices(ads) 
    {
      (vertexId, attr, ad) => 
      {
        if (ad.isEmpty)
        {
          new VertexAttributes(attr.pages, List.empty, attr.step, attr.inDegree, attr.outDegree)
        }
        else
        {
          new VertexAttributes(attr.pages, List(Ad(ad.get.id, ad.get.tokens, ad.get.next, ad.get.score)), 
                               attr.step, attr.inDegree, attr.outDegree)
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
  //
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
  //
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
  //
  // MIGRATION
  //
  // 1.  Decide which ads want to migrate to adjacent vertices
  // 2.  Send lists of migrating ads to adjacent vertices
  // 3.  Receive messages, add incoming ads 
  //
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
  def outputGDF(g:Graph[VertexAttributes, Int]) =
  {
    println("********** OUTPUT GDF FILE **********")

    // Output the nodes
    //"nodedef>name VARCHAR, color VARCHAR\n"
    val nodeRdd = g.vertices.map(v => if(v._2.score() == 0)(v._1 + ",'0,0,255'") else (v._1 + ",'255,0,0'"))
    nodeRdd.saveAsTextFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/nodes.gdf")

    // Output the edges
    //"edgedef>node1 VARCHAR, node2 VARCHAR\n"
    val edgeRdd = g.edges.map(e => (e.srcId + "," + e.dstId))
    edgeRdd.saveAsTextFile("hdfs://ip-172-31-4-59:9000/user/butterflies/data/edges.gdf")
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

} // end of class Simulator

////////////////////////////////////////////////////////////////////////////////////////////////


