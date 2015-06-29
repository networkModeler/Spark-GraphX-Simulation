////////////////////////////////////////////////////////////////////////////////
//
// File:	DataScaling.cpp
//
// Purpose:	Generate a larger dataset from the Kaggle 2012 Challenge Cup dataset
//
// Comment:	Original data set has a little over 26 million nodes (i.e. queries)
//			Adds new nodes that are linked to randomly selected existing nodes
//			Outputs an edgelist file in the format for Spark GraphX input
//
//			Command line arguments:
//			-degree : number of nodes to combine into a new node
//			-levels : number of levels of new nodes
//			-output : name for output edgelist file
//
// Author:	Dale Wong
//
////////////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <string>
#include <iostream>     // cout
#include <fstream>	    // ifstream, ofstream
#include <sstream>      // stringstream
#include <vector>

#include "Node.h"
#include "DataScaling.h"

using namespace std;
using namespace node;
using namespace dataScaling;

////////////////////////////////////////////////////////////////////////////////
// MAIN
////////////////////////////////////////////////////////////////////////////////

// Main entry point for program
int main(int argc, char* argv[])
{
    DataScaling myApp;

	// Parse the command line arguments
    if (!myApp.processArguments(argc, argv))
    {
        return(1);
    }

	// Generate the data set
    if (!myApp.generate())
    {
        return(1);
    }

	return(0);
}

////////////////////////////////////////////////////////////////////////////////
namespace dataScaling {
////////////////////////////////////////////////////////////////////////////////

// Constructor
DataScaling::DataScaling() : 
	maxWords(10000),
	wordsPerNode(10),
	degree(10), 
	levels(5),
	nodesPerLevel(100000),
	nodeCount(0),
	edgeCount(0),
	kaggleFile("100K_pages.txt"),
	nodeFile("100K_nodes.txt"),
	edgeFile("100K_edges.txt"),
	adFile("100K_ads.txt")
{
    // NA
}

// Destructor
DataScaling::~DataScaling()
{
	// Free the node data structure
	for (int i = 1; i < this->levels; ++i)
	{
		if (this->nodes[i])
		{
			delete this->nodes[i];
		}
	}
}

// Parse command line arguments
bool DataScaling::processArguments(int argc, char* argv[])
{
    if ((argc != 1) && (argc != 5))
    {
        cout << "Usage:  DataScaling <kaggleFile> <nodeFile> <edgeFile> <adFile>" << endl;
        return(false);
    }

	if (argc == 5)
	{
		this->kaggleFile = argv[1];
		this->nodeFile = argv[2];
		this->edgeFile = argv[3];
		this->adFile = argv[4];
	}

	return(true);
}

// Generate an expanded data set
bool DataScaling::generate()
{
	// Open the files
	ifstream kaggleFile;
	ofstream nodeFile;
	ofstream edgeFile;
	ofstream adFile;
	if (!this->openFiles(kaggleFile, nodeFile, edgeFile, adFile))
	{
		return(false);
	}

	// Initialize the node data structure
	this->initNodes();

	// Initialize the random number generator, used in generateLevel()
	srand(0);

	// Create the new nodes
	this->generateFirstLevel(kaggleFile);
	for (int i = 1; i < this->levels; ++i)
	{
		this->generateNextLevel(i);
		this->saveLevel((i - 1), nodeFile, edgeFile, adFile);
	}
	this->saveLevel((this->levels - 1), nodeFile, edgeFile, adFile);

	// Close the files
	kaggleFile.close();
	nodeFile.close();
	edgeFile.close();
	adFile.close();

	// Print some statistics
	cout << "number of nodes: " << this->nodeCount << endl;
	cout << "number of edges: " << this->edgeCount << endl;
	cout << "average degree: " << this->edgeCount / this->nodeCount << endl;

	return(true);
}

// Open the various files for output
bool DataScaling::openFiles(ifstream& kaggleFile, ofstream& nodeFile, ofstream& edgeFile, ofstream& adFile)
{
	kaggleFile.open(this->kaggleFile);
	if (!kaggleFile.is_open())
	{
		cout << "Failed to open input kaggle file:" << this->kaggleFile << endl;
		return(false);
	}

	nodeFile.open(this->nodeFile);
	if (!nodeFile.is_open())
	{
		cout << "Failed to open output node file:" << this->nodeFile << endl;
		return(false);
	}

	edgeFile.open(this->edgeFile);
	if (!edgeFile.is_open())
	{
		cout << "Failed to open output edge file:" << this->edgeFile << endl;
		return(false);
	}

	adFile.open(this->adFile);
	if (!adFile.is_open())
	{
		cout << "Failed to open output ad file:" << this->adFile << endl;
		return(false);
	}

	return(true);
}

// Initialize the node data structure
bool DataScaling::initNodes()
{
	for (int i = 0; i < this->levels; ++i)
	{
		this->nodes.push_back(new NodeList);
	}

	return(true);
}


// Read the Kaggle file into a vector of nodes
bool DataScaling::generateFirstLevel(ifstream& kaggleFile)
{
	cout << "Level:0" << "\t";

        // Set the maximum number of nodes per block, so a block can be processed in memory
        int maxNodes = 1024 * 64;                       // must be a power of two
        int maxNodesFlag = maxNodes - 1;        // bit trick to avoid modulo later

        // Get the nodeList for level 0
        NodeList* nodeList = this->nodes[0];

        // Process each line in the kaggle file
        int block = 0;
	stringstream ss;
	string buffer;
	string token;
        while (getline(kaggleFile, buffer))
        {
                // C++ stuff so we can tokenize the line
                ss.clear();
                ss.str("");
                ss << buffer;

                // The node id is always the first token on the kaggle line
                ss >> token;
                int nodeId = atoi(token.c_str());

                // Create a node object
                Node* node = new Node(nodeId);
                nodeList->push_back(node);

                // Parse this node's word list
                while (getline(ss, token, '|'))         // kaggle file delimits words with a '|' character
                {
                        int hashKeyOfWord = atoi(token.c_str());
                        node->addWord(hashKeyOfWord);
        	}

                // We need to process the input in blocks, can't fit all the nodes in memory

                // Check if we've read in a full block yet
                // Bit AND'ing with maxNodesFlag is a trick to avoid expensive modulo
                if ((nodeId > 0) && !(nodeId & maxNodesFlag))
                {
                        cout << "Block:" << ++block << "\t";
                        this->nodeCount += nodeList->size();
                }
        }

        // Process the last incomplete block
        {
                cout << "Block:" << ++block << "\t";
                this->nodeCount += nodeList->size();
        }

        cout << "Level:1\tnodeCount:" << this->nodeCount << endl;

    return(true);
}

/*
// Generate a new level of nodes
bool DataScaling::generateFirstLevel()
{
	cout << "Level:0" << "\t";

	// Set the number of new nodes to generate
	int numNewNodes = this->nodesPerLevel;

	// Set the number of clusters
	int numClusters = numNewNodes / this->degree;

	// Create the clusters
	for (int cluster = 0; cluster < numClusters; ++cluster)
	{
		// Create the word list for this cluster
		wordList clusterWords;
		int numClusterWords = 2 * this->wordsPerNode;

		for (int numWords = 0; numWords < numClusterWords; ++numWords)
		{
			// Add a random word
			clusterWords.push_back(rand() % this->maxWords);
		}

		// Create new random nodes
		NodeList* currentNodeList = this->nodes[0];
		for (int count = 0; count < numNewNodes; ++count)
		{
			// Create a new node
			int newId = this->nodeCount + count;	// nodeCount is total number of nodes in all previous levels
			Node* newNode = new Node(newId);
			currentNodeList->push_back(newNode);

			// Give the node some random words
			for (int numWords = 0; numWords < this->wordsPerNode; ++numWords)
			{
				int index = rand() % numClusterWords;
				int word = clusterWords[index];
				newNode->addWord(word);
			}
		}
	}

	// Update the total number of nodes so far
	this->nodeCount += numNewNodes;
	cout << "nodeCount:" << this->nodeCount << endl;

	return(true);
}
*/

// Generate a new level of nodes
bool DataScaling::generateNextLevel(int level)
{
	cout << "Level:" << level << "\t";

	// Get the number of nodes on the previous level
	NodeList* previousNodeList = this->nodes[level - 1];
	int previousSize = previousNodeList->size();

	// Set the number of new nodes to generate
	int numNewNodes = this->nodesPerLevel;

	// Create new random nodes
	NodeList* currentNodeList = this->nodes[level];
	for (int count = 0; count < numNewNodes; ++count)
	{
		// Create a new node
		int newId = this->nodeCount + count;	// nodeCount is total number of nodes in all previous levels
		Node* newNode = new Node(newId);
		currentNodeList->push_back(newNode);

		// Randomly select the degree for this node
		int nodeDegree = (rand() % this->degree) + 1;

		// For each degree (as specified on the command line)
		for (int degree = 0; degree < nodeDegree; ++degree)
		{
			// Select a random node from the previous level
			int oldId = rand() % previousSize;

			// Check if there's already an edge to the selected node
			if (!newNode->findEdge(oldId))
			{
				Node* oldNode = previousNodeList->at(oldId);

				// Add an edge between the new node and the old node
				newNode->addEdge(oldId);
				oldNode->addEdge(newId);
				this->edgeCount += 2;

				// Randomly add some of the old node's words into the new node
				int numWords = this->wordsPerNode / nodeDegree;
				newNode->addPartialWordList(oldNode, numWords);
			}
		}
	}

	// Update the total number of nodes so far
	this->nodeCount += numNewNodes;
	cout << "nodeCount:" << this->nodeCount << endl;

	return(true);
}

// Process a block of nodes
bool DataScaling::saveLevel(int level, ofstream& nodeFile, ofstream& edgeFile, ofstream& adFile)
{
        // Get this level's list of nodes
        NodeList* nodeList = this->nodes[level];

	// Find and create edges between nodes on the same level
	this->makeSiblingEdges(nodeList, level);

	// Flush this block from memory
	for (unsigned int i = 0; i < nodeList->size(); ++i)
	{
		Node* node = nodeList->at(i);

		// Output node to the nodeFile
		node->print(nodeFile);

		// Output a matching ad to the adFile
		// "Known truth" allows us to measure quality of the system
		node->print(adFile);

		// Output this node's edges to the edgeFile
		node->printEdges(edgeFile);
		
		// Deallocate the node
		delete node;
	}

	// Empty the node list
	nodeList->clear();

	return(true);
}

// Find and make edges between nodes on the same level
bool DataScaling::makeSiblingEdges(NodeList* nodeList, int level)
{
        // Set the similarity threshold for an edge
        int threshold = 5;
        //if (level == 0)
        //{
	//	threshold = 1;
	//}

	// Check every possible pair of nodes
	int count = 0;
	for (auto itNode1 = nodeList->begin(); itNode1 != nodeList->end(); ++itNode1)
	{
		Node* node1 = *itNode1;
		int id1 = node1->getId();

		for (auto itNode2 = nodeList->begin(); itNode2 != nodeList->end(); ++itNode2)
		{
			Node* node2 = *itNode2;
			int id2 = node2->getId();

			// Just check the upper triangle of possible pairs
			if (id2 > id1)
			{
				// Check if they have at least one word in common
				if (node1->isCommon(node2))
				{
					if (++count >= threshold)
					{
						// Add an edge between the two nodes
						node1->addEdge(id2);
						node2->addEdge(id1);
						this->edgeCount += 2;

						return(true);
					}
				}
			}
		}
	}

	return(false);
}

// Print the node data structure
bool DataScaling::printNodes()
{
	for (int i = 0; i < this->levels; ++i)
	{
		cout << "Level:" << i << endl;

		// Get the nodeList for this level
		NodeList* nodeList = this->nodes[i];

		for (auto it = nodeList->begin(); it != nodeList->end(); ++it)
		{
			Node* node = *it;
			node->print();
		}
	}

	return(true);
}

////////////////////////////////////////////////////////////////////////////////
}   // namespace dataScaling
////////////////////////////////////////////////////////////////////////////////

