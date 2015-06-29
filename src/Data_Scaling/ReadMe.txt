Creates "web pages" and "ads" in the style of the Kaggle 2012 Challenge Cup

- Read in the pages from the Kaggle file
- Generate additional pages, linked to random pages 
- Similar pages (those with common words) will be connected by an edge
- A matching ad is created for each page (both have the same list of words)
- This "ground truth" lets us measure the quality of the system

Also outputs an edgelist file in the format for Spark GraphX input
