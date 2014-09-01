/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     arayshu, lschneid - initial implementation
 */

#include <common/skv_utils.hpp>

#include <stdlib.h>
#include <fstream>
#include <string>
#include <iostream>

using namespace std;

int GetBGPRank()
{
  ifstream fin("/etc/personality");
  
  int MAX_LENGTH = 100;
  char line[ MAX_LENGTH ];
  
  int RankFound = 0;
  int Rank = -1;
  while( fin.getline( line, MAX_LENGTH ) )
  {
    string str( line );

    string::size_type loc = str.find( "BG_RANK=", 0 );

    if( loc != string::npos )
    {
      Rank = (int) atoi( str.substr( 8 ).c_str() );
      RankFound = 1;
      break;
    }
  }

  return Rank;
}

int GetBGPPartitionSize()
{
  ifstream fin("/etc/personality");
  
  int MAX_LENGTH = 100;
  char line[ MAX_LENGTH ];
  
  int NodesInPset      = -1;
  int NodesInPsetFound =  0;

  int NumPsets      = -1;
  int NumPsetsFound =  0;

  while( fin.getline( line, MAX_LENGTH ) )
  {
    string str( line );

    string::size_type loc1 = str.find( "BG_NODESINPSET=", 0 );

    string::size_type loc2 = str.find( "BG_NUMPSETS=", 0 );

    if( loc1 != string::npos )
    {
      NodesInPset = (int) atoi( str.substr( 15 ).c_str() );
      NodesInPsetFound = 1;
    }
    else if( loc2 != string::npos )
    {
      NumPsets = (int) atoi( str.substr( 12 ).c_str() );
      NumPsetsFound = 1;
    }
  }

  if( NodesInPsetFound && NumPsetsFound )
  {
    return (NumPsets * NodesInPset);
  }
  else
  {
    return -1;
  }
}

