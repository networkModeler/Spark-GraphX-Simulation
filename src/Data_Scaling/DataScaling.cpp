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
	degree(10), 
	levels(10), 
	kaggleFile(""), 
	nodeFile(""),
	edgeFile(""), 
	//nodes(NULL), 
	nodeCount(0)
{
    // NA
}

// Destructor
DataScaling::~DataScaling()
{
	// Free the node data structure
	for (int i = 1; i < this->levels; ++i)
	{
		delete this->nodes[i];
	}
}

// Parse command line arguments
bool DataScaling::processArguments(int argc, char* argv[])
{
    if (argc != 6)
    {
        cout << "Usage:  DataScaling <degree> <levels> <inputFile> <nodeFile> <edgeFile>" << endl;
        return(false);
    }

    this->degree = atoi(argv[1]);
    this->levels = atoi(argv[2]);
    this->kaggleFile = argv[3];
	this->nodeFile = argv[4];
	this->edgeFile = argv[5];

	return(true);
}

// Generate an expanded data set
bool DataScaling::generate()
{
	// Open the files
	ifstream kaggleFile;
	ofstream nodeFile;
	ofstream edgeFile;
	if (!this->openFiles(kaggleFile, nodeFile, edgeFile))
	{
		return(false);
	}

	// Initialize the node data structure
	this->initNodes();

	// Initialize the random number generator, used in generateLevel()
	srand(0);

	// Read in the Kaggle file, output the node and edge files
	this->parseFile(kaggleFile, nodeFile, edgeFile);

	// Create the new nodes
	for (int i = 2; i < this->levels; ++i)
	{
		this->generateLevel(i);
		this->processLevel((i - 1), nodeFile, edgeFile);
	}
	this->processLevel((this->levels - 1), nodeFile, edgeFile);

	// Close the files
	kaggleFile.close();
	nodeFile.close();
	edgeFile.close();

	return(true);
}

// Read the Kaggle file into a vector of nodes
bool DataScaling::openFiles(ifstream& kaggleFile, ofstream& nodeFile, ofstream& edgeFile)
{
	kaggleFile.open(this->kaggleFile);
	if (!kaggleFile.is_open())
	{
		cout << "Failed to open input Kaggle data file:" << this->kaggleFile << endl;
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
bool DataScaling::parseFile(ifstream& kaggleFile, ofstream& nodeFile, ofstream& edgeFile)
{
	// Set the maximum number of nodes per block, so a block can be processed in memory
	//int maxNodes = 1024 * 1024;			// must be a power of two
	int maxNodes = 1024 * 64;			// must be a power of two
	int maxNodesFlag = maxNodes - 1;	// bit trick to avoid modulo later

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
		while (getline(ss, token, '|'))		// kaggle file delimits words with a '|' character
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
			this->processBlock(nodeList, nodeFile, edgeFile);
		}
	}

	// Process the last incomplete block
	{
		cout << "Block:" << ++block << "\t";
		this->nodeCount += nodeList->size();
		this->processBlock(nodeList, nodeFile, edgeFile);
	}

	cout << "Level:1\tnodeCount:" << this->nodeCount << endl;

	// Echo to verify
	//this->printNodes();

    return(true);
}

// Process a block of nodes
bool DataScaling::processBlock(NodeList* nodeList, ofstream& nodeFile, ofstream& edgeFile)
{
	// Find and create edges between nodes on the same level
	this->makeSiblingEdges(nodeList);

	// Generate the next level for this block
	this->generateLevel(1);

	// Flush this block from memory
	for (unsigned int i = 0; i < nodeList->size(); ++i)
	{
		Node* node = nodeList->at(i);

		// Output node to the nodeFile
		node->print(nodeFile);

		// Output this node's edges to the edgeFile
		node->printEdges(edgeFile);
		
		// Deallocate the node
		delete node;
	}

	// Empty the node list
	nodeList->clear();

	return(true);
}

// Process a block of nodes
bool DataScaling::processLevel(int level, ofstream& nodeFile, ofstream& edgeFile)
{
	NodeList* nodeList = this->nodes[level];
	unsigned int size = nodeList->size();

	// Flush this block from memory
	for (unsigned int i = 0; i < size; ++i)
	{
		Node* node = nodeList->at(i);

		// Output node to the nodeFile
		node->print(nodeFile);

		// Output this node's edges to the edgeFile
		node->printEdges(edgeFile);
		
		// Deallocate the node
		delete node;
	}

	// Empty the node list
	nodeList->clear();

	return(true);
}

// Generate a new level of nodes
bool DataScaling::generateLevel(int level)
{
	if (level == 1)
	{
		cout << "Level:0 \t";
	}
	else
	{
		cout << "Level:" << level << "\t";
	}

	// Get the number of nodes on the previous level
	NodeList* previousNodeList = this->nodes[level - 1];
	int previousSize = previousNodeList->size();

	// Set the number of new nodes to generate
	//int numNewNodes = previousSize / 2;
	int numNewNodes = min(100000, previousSize);

	// Create new random nodes
	NodeList* currentNodeList = this->nodes[level];
	for (int count = 0; count < numNewNodes; ++count)
	{
		// Create a new node
		int newId = this->nodeCount + count;	// nodeCount is total number of nodes in all previous levels
		Node* newNode = new Node(newId);
		currentNodeList->push_back(newNode);

		// For each degree (as specified on the command line)
		for (int degree = 0; degree < this->degree; ++degree)
		{
			// Select a random node from the previous level
			int oldId = rand() % previousSize;

			// Check if there's already edge to the selected node
			if (!newNode->findEdge(oldId))
			{
				Node* oldNode = previousNodeList->at(oldId);

				// Add an edge between the new node and the old node
				newNode->addEdge(oldId);
				oldNode->addEdge(newId);

				// Add the old node's words into the new node
				//newNode->addWordList(oldNode);
				newNode->addPartialWordList(oldNode, this->degree);
			}
		}
	}

	// Update the total number of nodes so far
	this->nodeCount += currentNodeList->size();
	cout << "nodeCount:" << this->nodeCount << endl;

	return(true);
}

// Find and make edges between nodes on the same level
bool DataScaling::makeSiblingEdges(NodeList* nodeList)
{
	// Check every possible pair of nodes
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
					// Add an edge between the two nodes
					node1->addEdge(id2);
					node2->addEdge(id1);
				}
			}
		}
	}

	return(true);
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
