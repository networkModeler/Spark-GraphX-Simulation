////////////////////////////////////////////////////////////////////////////////
//
// File:	DataScaling.h
//
// Purpose:	header file for the DataScaling class
//
// Author:	Dale Wong
//
////////////////////////////////////////////////////////////////////////////////

#ifndef	DATASCALING_H
#define	DATASCALING_H

#include <fstream>	    // ifstream, ofstream
#include "Node.h"

using namespace node;

////////////////////////////////////////////////////////////////////////////////

namespace dataScaling
{
	typedef vector<Node*> NodeList;
	typedef vector<NodeList*> LevelList;

	class DataScaling
	{
		private:
			int			maxWords;
			int			wordsPerNode;
			int			degree;
			int			levels;
			int			nodesPerLevel;
			int			nodeCount;
			int			edgeCount;
			string		kaggleFile;
			string		nodeFile;
			string		edgeFile;
			string		adFile;
			LevelList	nodes;

			bool initNodes();
			bool openFiles(ifstream& kaggleFile, ofstream& nodeFile, ofstream& edgeFile, ofstream& adFile);
			bool generateFirstLevel(ifstream& kaggleFile);
			bool generateNextLevel(int level);
			bool saveLevel(int level, ofstream& nodeFile, ofstream& edgeFile, ofstream& adFile);
			bool makeSiblingEdges(NodeList* nodeList, int level);
			bool printNodes();

		public:
			DataScaling();
			~DataScaling();
			bool processArguments(int argc, char* argv[]);
			bool generate();
	};
}

////////////////////////////////////////////////////////////////////////////////
#endif	// DATASCALING_H
////////////////////////////////////////////////////////////////////////////////

