/**
 *
 * PAGE CLASS: class for a single page
 *
 * Stores the bag of words comprising a page's description.
 * Each vertex in the graph initially corresponds to a single page.
 * After the replication phase, each vertex stores a list of its neighboring pages.
 *
 */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * class for a single page
 *
 * @param id: unique identifier for this page
 * @param tokens: list of words from this page's description
 * @param score: similarity score for this page and an ad, used as scratchpad by the ad
 */
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


