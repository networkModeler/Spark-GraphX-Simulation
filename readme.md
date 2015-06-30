## A Nature-Inspired Algorithm Implemented with Spark GraphX ##

An Insight Data Engineering Demonstration Project by Dale Wong

### Overview ###

I investigated the implementation of a computationally demanding algorithm on a "distributed graph-parallel computation" system.  The GraphX system is Spark's implementation of the Google Pregel architecture where essentially every node in a graph can be programmed as if it were executing its own MapReduce program.  Further information on Spark GraphX can be found here:  [https://amplab.github.io/graphx/]()

I elected to implement a nature-inspired algorithm for this study.  The algorithm is a good fit for graph-parallel systems because the computations consist of localized decisions, albeit performed in parallel by a "swarm" of thousands of individuals.

The particular application of this algorithm is AdTech.

### The Nature-Inspired Algorithm ###

Suppose we are given a collection of web pages and a collection of ads, where each page has its own list of words that it contains and each ad likewise has its own list of words that it contains.

We form a "similarity graph" of the pages, where a node is created for each page, and an edge is created between each pair of similar nodes.  For the nature-inspired algorithm, we think of this graph as a thicket of branches to be traversed.

For each ad, we create a "butterfly" object.  Butterflies reside on a node, and migrate along branches connected to that node.

The flight of the butterflies is a function of the local attraction and repulsion of the neighboring branches, plus some randomization to escape local minima.

To initialize the system, we randomly assign butterflies to nodes.  We then run the system for a number of cycles, letting the butterflies migrate.  The objective is for the system to converge on a state where ads hover around relevent pages.

![similarity graph](./doc/similarity_graph.png?raw=true "similarity graph")

Please note that while this project used the simplest possible feature set and similarity measure, these are encapsulated by functions and can easily be changed to explore more sophisticated feature sets and metrics.

### Results ###

The following snapshots show the state of the system at times 0, 100, and 1000.  The displayed color of each node indicates the average similarity between the node and its resident ads:  blue indicates no similarity, and red indicates similarity.

![results](./doc/results.gif?raw=true "results")

There are many possible enhancements to the algorithm that could improve the quality of results (e.g. multiplying successful ads, exiling unsuccessful ads, turf wars in crowded nodes, etc.), but these fall more in the realm of data science as opposed to data engineering.

### Implementation Details ###

Following are some noteworthy implementation decisions:

- **Node-Oriented Versus Edge-Oriented**:  The nature-inspired algorithm is node-oriented.  Each butterfly must decide which one of a node's outgoing edges to select for the next leg of his flight.  On the other hand, the Spark GraphX Pregel API is edge-oriented.  In the diagram below of the Pregel API, the point to note is that in Step 1, messages are sent for each edge rather than for each node.  At no time is there visibility from a node to all its adjacent nodes:

![results](./doc/Pregel.pdf?raw=true "results")

In order to adapt the algorithm to the Pregel API, I elected to replicate a node's features onto each of its adjacent neighbors.  This replication was done for all nodes during an initialization phase of the algorithm, and was itself implemented by its own Pregel "program".

![replicate](./doc/replicate.gif?raw=true "replicate")

This replication scheme enabled the algorithm to make self-contained migration calculations during Step 3 ("Vertex Program") of the Pregel API.  Clearly the replication increases the storage required by the system, but it also dramatically decreases the amount of inter-node traffic during the algorithm execution.  I believe this is the proper trade-off for a graph based system where the communication is between widely distributed nodes and there is no clean partitioning scheme.

- **Weighted Random Edge Selection**:  When selecting the next edge for a butterfly's flight, the algorithm does not greedily pick the edge with the highest similarity score.  Such a greedy selection strategy could trap a butterfly in a local minima and prevent it from exploring the graph for what may be an even better match.  Instead, the algorithm randomly selects an edge with a probability that is proportional to the edge's similarity score.  Thus the butterfly will tend towards the better edges, but does not eliminate any paths.  The Scala code is as follows:

>     // Select a page, randomly in proportion to the page's score
>     def selectWeightedRandom(pageList: List[Page], currentPage: VertexId): {
> 
>       // Check if there are no neighboring vertices
>       if (pageList.length == 1) {
>         return pageList(0).id
>       }
>       
>       // Create a distribution normalized from 0 to 1
>       val sum = pageList.map(_.score + 1.0).sum
>       val distribution = pageList.map(page => (page.id, (page.score + 1.0) / sum)).toMap
> 
>       // Get a random number
>       val randomNumber = scala.util.Random.nextDouble
> 
>       // See where the random number lands amongst the pages
>       var accumulator = 0.0
>       val it = distribution.iterator
>       while (it.hasNext) {
>         val (page, score) = it.next
>         accumulator += score
>         if (accumulator >= randomNumber) {
>           selectedPage = page
>         }
>       }
> 
>       return selectedPage
>     }

- **Similarity Graph Construction**:  Scala provides rich primitives and concise expression.  In particular for this algorithm, the construction of the similarity graph can be implemented in just two lines of Scala:

>       val allPairs = pages.cartesian(pages).filter{ case (a, b) => a._1 < b._1 }
>       val similarPairs = allPairs.filter{ case (page1, page2) => page1._2.intersect(page2._2).length >= 1 }

Of course, no matter how concisely it's expressed, this is still an N^2 operation.  Because the graph is static for this project, I elected to generate the graph once, save the edges in a Spark GraphX edgelist file, and then simply reload this file to start the simulation.

- **Final Comment on Spark GraphX**: The Google Pregel model of graph-parallel computation is a natural fit for distributed computing and certain classes of computationally intensive algorithms.  The model will inevitably propagate into data centers in the near future.  However, the Spark GraphX package is still relatively immature.  It's current implementation of the Pregel API is limited and inflexible.  Based on my experience and the amount of coercion required for implementation, I would recommend looking instead at other implementations of the Pregel model, such as the GraphLab package.