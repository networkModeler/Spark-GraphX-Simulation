/**
 * AD CLASS: class for a single ad
 *
 * Next page selection:
 *	The scorePages method is called from the vertex program of the Pregel super step.
 *	The calling vertex also passes in a list of its adjacent neighbors.
 *	The ad then scores itself against each of the vertices, and remembers its selection.
 *	The ad's selection is then used during the message sending step of the Pregel super step.
 *
 *	The selection is made randomly, in proportion to the similarity metric for each page.
 *	This biases the selection towards better pages, but does not exclude exploring other pages,
 *	allowing the ad to escape local minima.
 */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Define a class for an ad
 *
 * @param id: unique identifier for this ad
 * @param tokens: list of words in this ad's description
 * @param next: the vertex to migrate to next, used as scratchpad to communicate between the
 *		vertexProgram and sendMessage steps of a super-step
 * @param score: the similarity score between this ad and the next vertex, used for debug only
 */
case class Ad(id: Long, tokens: List[String], var next: VertexId, var score: Int) extends java.io.Serializable
{
  val debug = true

  def similarity(page: Page) : Int =
  {
     val commonList = this.tokens.intersect(page.tokens)
     val numCommon = commonList.length
     return numCommon
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Select a page from the given list of unscored pages
   */
  def scorePages(pageList: List[Page], currentPage: VertexId) =
  {
    // Score this ad with each page in the given list
    pageList.foreach( page => page.score = page.similarity(this) )

    // Greedily select the page with the highest similarity score
    //this.next = pageList.maxBy(_.score).id

    // Randomly select a page, weighted by their similarity scores
    this.next = this.selectWeightedRandom(pageList, currentPage)

    val filtered = pageList.filter(_.id == this.next)
    val page = filtered(0)
    val score = page.score
    this.score = score

    // Statistics 
    if (debug)
    {
      if (filtered.length > 0)
      {
        println("********** scorePages, node=" + currentPage + " **********")
        pageList.foreach(println)

        println("migrate ad " + this.id + " from page " + currentPage + " to page " + this.next + ", score " + score)
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Select a page from the given list of scored pages, randomly in proportion to the page's score
   */
  def selectWeightedRandom(pageList: List[Page], currentPage: VertexId): VertexId =
  {
    // Check if there are no neighboring vertices
    if (pageList.length == 1)
    {
      return pageList(0).id
    }
    // Create a distribution normalized from 0 to 1
    val sum = pageList.map(_.score + 1.0).sum   // + 1.0 because many scores will be zero
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
    }

    // We land here if there are duplicates in the pageList
    // Greedily select the page with the highest similarity score
    if (selectedPage == currentPage)
    {
      selectedPage = pageList.maxBy(_.score).id
    }

    return selectedPage
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////


