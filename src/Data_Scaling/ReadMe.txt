Creates a larger data set based on the Kaggle 2012 Challenge Cup track 2 data set

Original data set has a little over 26 million nodes (i.e. queries)

Adds new nodes that are linked to randomly selected existing nodes

Outputs an edgelist file in the format for Spark GraphX input

Command line arguments (by position, not by flag):
number of nodes to combine into a new node
number of levels of new nodes
name for Kaggle input file
name for output node file
name for output edgelist file