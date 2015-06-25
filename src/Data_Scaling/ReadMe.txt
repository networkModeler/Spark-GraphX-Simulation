Creates "web pages" and "ads" in the style of the Kaggle 2012 Challenge Cup

- Pages are created with a random list of words
- Similar pages (those with common words) will be connected by an edge
- A matching ad is created for each page (both have the same list of words)
- This "known truth" lets us measure the quality of the system

Outputs an edgelist file in the format for Spark GraphX input
