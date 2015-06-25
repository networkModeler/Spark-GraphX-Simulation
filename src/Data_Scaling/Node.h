////////////////////////////////////////////////////////////////////////////////
//
// File:	Node.h
//
// Purpose:	header file for the Node class
//
// Author:	Dale Wong
//
////////////////////////////////////////////////////////////////////////////////

#ifndef	NODE_H
#define	NODE_H

#include <fstream>	    // ifstream, ofstream
#include <vector>

using namespace std;

////////////////////////////////////////////////////////////////////////////////

namespace node
{
	typedef vector<int> wordList;	// In the Kaggle files, words are just the hash key
	typedef vector<int> edgeList;	// The id of the linked node

	class Node
	{
		private:
			int id;
			wordList words;
			edgeList edges;


		public:
			Node(int id);
			int  getId();
			bool findWord(int word);
			bool addWord(int word);
			bool addWordList(Node* otherNode);
			bool addPartialWordList(Node* otherNode, int numWords);
			bool isCommon(Node* otherNode);
			bool addEdge(int id);
			bool findEdge(int id);
			bool printEdges(ofstream& edgeFile);
			bool print(ofstream& nodeFile);
			bool print();
	};
}

////////////////////////////////////////////////////////////////////////////////
#endif	// NODE_H
////////////////////////////////////////////////////////////////////////////////