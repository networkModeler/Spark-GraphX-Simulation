////////////////////////////////////////////////////////////////////////////////////////////////
//
// VERTEXATTRIBUTES CLASS: the user defined class which is joined to each vertex in the graph
//
// Each vertex maintains a list of neighboring pages, and a list of Ads currently residing on the vertex.
// This class also holds additional debug information.
//
////////////////////////////////////////////////////////////////////////////////////////////////

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

////////////////////////////////////////////////////////////////////////////////////////////////

// Define a class for GraphX vertex attributes
class VertexAttributes(var pages: List[Page], var ads: List[Ad], var step: Long, val inDegree: Int, val outDegree: Int)
extends java.io.Serializable
{
  // Define alternative methods to be used as the score
  def averageScore() =
  {
    if (this.ads.length == 0) 0 else this.ads.map(_.score).sum / this.ads.length
  }

  def maxScore() =
  {
    if(this.ads.length == 0) 0 else this.ads.map(_.score).max
  }

  // Select averageScore as the function to be used
  val score = averageScore _
}

////////////////////////////////////////////////////////////////////////////////////////////////


