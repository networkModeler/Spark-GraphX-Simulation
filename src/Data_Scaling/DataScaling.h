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
			int			degree;
			int			levels;
			string		kaggleFile;
			string		nodeFile;
			string		edgeFile;
			LevelList	nodes;
			int			nodeCount;

			bool initNodes();
			bool openFiles(ifstream& kaggleFile, ofstream& nodeFile, ofstream& edgeFile);
			bool parseFile(ifstream& kaggleFile, ofstream& nodeFile, ofstream& edgeFile);
			bool processBlock(NodeList* nodeList, ofstream& nodeFile, ofstream& edgeFile);
			bool generateLevel(int level);
			bool processLevel(int level, ofstream& nodeFile, ofstream& edgeFile);
			bool makeSiblingEdges(NodeList* nodeList);
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