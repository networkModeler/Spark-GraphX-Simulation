////////////////////////////////////////////////////////////////////////////////
//
// File:	Node.cpp
//
// Purpose:	Node class methods
//
// Author:	Dale Wong
//
////////////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <string>
#include <iostream>     // cout
#include <algorithm>	// find

#include "Node.h"

using namespace std;

////////////////////////////////////////////////////////////////////////////////
namespace node {
////////////////////////////////////////////////////////////////////////////////

// Constructor
Node::Node(int id)
{
    this->id = id;
}

int Node::getId()
{
	return(this->id);
}

bool Node::findWord(int word)
{
	if (std::find(this->words.begin(), this->words.end(), word) != this->words.end())
	{
		return(true);
	}
	else
	{
		return(false);
	}
}

bool Node::addWord(int word)
{
	this->words.push_back(word);
	return(true);
}

bool Node::addWordList(Node* otherNode)
{
	for (auto it = otherNode->words.begin(); it != otherNode->words.end(); ++it)
	{
		int word = *it;
		// Check if this word is already in this node's word list
		if (!this->findWord(word))
		{
			this->addWord(word);
		}
	}
	return(true);
}

bool Node::addPartialWordList(Node* otherNode, int degree)
{
	// Set a limit to how many words to copy
	int maxNumWords = 3 * degree;

	// Calculate how many words to copy, based on the degree of the new node
	int numWords = otherNode->words.size();

	// Check if we should just copy all the words
	if (numWords <= maxNumWords)
	{
		for (auto it = otherNode->words.begin(); it != otherNode->words.end(); ++it)
		{
			int word = *it;

			// Check if this word is already in this node's word list
			if (!this->findWord(word))
			{
				this->addWord(word);
			}
		}
	}

	// Select the words randomly
	else
	{
		int numWordsToCopy = maxNumWords;

		// Randomly pick the words
		for (int count = 0; count < numWordsToCopy; ++ count)
		{
			int randomIndex = rand() % numWords;
			int word = otherNode->words.at(randomIndex);

			// Check if this word is already in this node's word list
			if (!this->findWord(word))
			{
				this->addWord(word);
			}
		}
	}

	return(true);
}

// Return true if any word is common to both nodes
bool Node::isCommon(Node* otherNode)
{
	for (auto it = this->words.begin(); it != this->words.end(); ++it)
	{
		int word = *it;

		if (otherNode->findWord(word))
		{
			return(true);
		}
	}

	return(false);
}

bool Node::addEdge(int id)
{
	this->edges.push_back(id);
	return(true);
}

bool Node::findEdge(int edge)
{
	if (std::find(this->edges.begin(), this->edges.end(), edge) != this->edges.end())
	{
		return(true);
	}
	else
	{
		return(false);
	}
}

// Print the node's edges
// Match the GraphX edge list file format
bool Node::printEdges(ofstream& edgeFile)
{
	for (auto it = this->edges.begin(); it != this->edges.end(); ++it)
	{
		int otherId = *it;
		edgeFile << this->id << " " << otherId << endl;
	}

	return(true);
}

// Print the node
// Match the Kaggle format
bool Node::print(ofstream& nodeFile)
{
	nodeFile << this->id;
	nodeFile << "\t";

	bool first = true;
	for (auto it = this->words.begin(); it != this->words.end(); ++it)
	{
		int word = *it;

		if (!first)
		{
			nodeFile << "|";
		}
		else
		{
			first = false;
		}

		nodeFile << word;
	}
	nodeFile << endl;

	return(true);
}

// Print the node
// Match the Kaggle format so we can diff to verify
bool Node::print()
{
	cout << this->id;
	cout << "\t";

	bool first = true;
	for (auto it = this->words.begin(); it != this->words.end(); ++it)
	{
		int word = *it;

		if (!first)
		{
			cout << "|";
		}
		else
		{
			first = false;
		}

		cout << word;
	}
	cout << endl;

	return(true);
}

////////////////////////////////////////////////////////////////////////////////
}   // namespace node
////////////////////////////////////////////////////////////////////////////////