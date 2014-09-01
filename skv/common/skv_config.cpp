/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * Contributors:
 *     lschneid - initial implementation
 */

#include <cstdlib>
#include <iostream>
#include <fstream>
#include <arpa/inet.h>

#include <FxLogger.hpp>

#ifndef SKV_CONFIG_FILE_LOG
#define SKV_CONFIG_FILE_LOG ( 0 )
#endif

#include <common/skv_config.hpp>

skv_configuration_t *skv_configuration_t::mConfiguration = NULL;

typedef enum
{
  SKV_CONFIG_FILE_EMPTY_LINE,
  SKV_CONFIG_FILE_COMMENT,
  SKV_CONFIG_FILE_ASSIGNMENT
} skv_config_line_status_t;

skv_config_line_status_t IsComment( string s );

/**********************************************************************/

// constructor to set default values
skv_configuration_t::skv_configuration_t( )
{
  mConfigFile = DEFAULT_CONFIG_FILE;

  mServerPort = DEFAULT_SKV_SERVER_PORT;

  mMachineFile = DEFAULT_SKV_MACHINE_FILE;

  mReadyFile = DEFAULT_SKV_SERVER_READY_FILE;
  
  mPersistentFilename      = DEFAULT_SKV_PERSISTENT_FILENAME;
  mPersistentFileLocalPath = DEFAULT_SKV_PERSISTENT_FILE_LOCAL_PATH;
        
  mLocalInfoFile = DEFAULT_SKV_LOCAL_SERVER_INFO_FILE;
}

// get the location and name of the config file
int
skv_configuration_t::GetConfigurationFile( const char *aConfigFile )
{

  ifstream cFileStream;
  // check if user provided a file
  // safe string length check: if there are more than N letters in the file name, we're fine
  if( (aConfigFile != NULL) && (strnlen( aConfigFile, 16 ) > 0) )
  {
    BegLogLine( SKV_CONFIG_FILE_LOG )
      << "skv_configuration_t::GetConfigurationFile()::"
      << " user provided file: " << aConfigFile
      << EndLogLine;

    // check if provided file is there
    cFileStream.open( aConfigFile, ifstream::in );

    if( cFileStream.good() )
    {
      cFileStream.close();
      mConfigFile = string( aConfigFile );

      return 0;
    }
    else
    {
      BegLogLine( SKV_CONFIG_FILE_LOG )
        << "skv_configuration_t::GetConfigurationFile():: user-provided config file not found: " << aConfigFile
        << EndLogLine;
    }

  }
          
  // check for home-dir default file ~/.skv.conf
  string tmpCfg( getenv( "HOME" ) );
  tmpCfg.append( "/.skv_server.conf" );

  BegLogLine( SKV_CONFIG_FILE_LOG )
    << "skv_configuration_t::GetConfigurationFile()::"
    << " user default file: " << tmpCfg.c_str()
    << EndLogLine;

  // check if file is there
  cFileStream.open( tmpCfg.c_str(), ifstream::in );

  if( cFileStream.good() )
  {
    cFileStream.close();
    mConfigFile = tmpCfg;

    return 0;
  }
  else
    BegLogLine( SKV_CONFIG_FILE_LOG )
        << "skv_configuration_t::GetConfigurationFile():: no server conf in home dir: " << tmpCfg.c_str()
        << EndLogLine;

  BegLogLine( SKV_CONFIG_FILE_LOG )
    << "skv_configuration_t::GetConfigurationFile()::"
    << " using system default config: " << mConfigFile.c_str()
    << EndLogLine;

  // just return the default system config file if nothing else was found
  // means: don't touch the default file name that was set in constructor
  return 0;
}


int
skv_configuration_t::ReadConfigurationFile( const char *aConfigFile )
{
  int status = GetConfigurationFile( aConfigFile );

  BegLogLine( SKV_CONFIG_FILE_LOG )
    << "skv_configuration_t::ReadConfigurationFile():: "
    << " got config file: " << mConfigFile.c_str()
    << " status: " << status
    << EndLogLine;

  if( status )
  {
    return status;
  }

  string cline;

  ifstream cFileStream;
  cFileStream.open( mConfigFile.c_str(), ifstream::in );

  if( cFileStream.is_open() )
  {

    BegLogLine( SKV_CONFIG_FILE_LOG )
      << "skv_configuration_t::ReadConfigurationFile():: "
      << " file open, now parsing: " << mConfigFile.c_str()
      << EndLogLine;

    int line_number = 0;
    while( cFileStream.good() )
    {
      size_t valueIndex = 0;

      getline( cFileStream, cline );
      line_number++;

      if( IsComment( cline ) == SKV_CONFIG_FILE_ASSIGNMENT )
      {
        skv_config_setting_t setting = GetVariableCase( cline, &valueIndex );

        BegLogLine( SKV_CONFIG_FILE_LOG )
          << "skv_configuration_t::ReadConfigurationFile():: Parsing line: " << line_number
          << " found case: " << (int)setting
          << " value col: " << valueIndex
          << EndLogLine;

        switch( GetVariableCase( cline, &valueIndex ) )
        {
          case SKV_CONFIG_SETTING_SERVER_PORT:
            SetSKVServerPort( (uint16_t)atoi( cline.substr( valueIndex ).c_str() ) );
            break;

          case SKV_CONFIG_SETTING_READY_FILE:
            mReadyFile = cline.substr( valueIndex );
            break;

          case SKV_CONFIG_SETTING_MACHINE_FILE:
            mMachineFile = cline.substr( valueIndex );
            break;

          case SKV_CONFIG_SETTING_PERSISTENT_FILENAME:
            mPersistentFilename = cline.substr( valueIndex );
            break;

          case SKV_CONFIG_SETTING_PERSISTENT_FILE_LOCAL_PATH:
            mPersistentFileLocalPath = cline.substr( valueIndex );
            break;

          case SKV_CONFIG_SETTING_LOCAL_SERVER_INFO_FILE:
            mLocalInfoFile = cline.substr( valueIndex );
            break;

          case SKV_CONFIG_SETTING_COMM_IF:
            mCommIF = cline.substr( valueIndex );
            break;

          default:
            BegLogLine( 1 )
              << "skv_configuration_t::ReadConfigurationFile():: unknown parameter in"
              << " line: " << line_number
              << " : " << cline.c_str()
              << EndLogLine;

        }
      }
      // else
      //   BegLogLine( SKV_CONFIG_FILE_LOG )
      //     << " line is comment or empty"
      //     << EndLogLine;

    }

    cFileStream.close();

  }
  else
  {
    BegLogLine( 1 )
      << "skv_configuration_t::ReadConfigurationFile()::"
      << " Error while opening config file:" << mConfigFile.c_str()
      << " Using DEFAULT PARAMETERS!"
      << EndLogLine;

    status = 0;
  }

  BegLogLine( SKV_CONFIG_FILE_LOG )
    << "skv_configuration_t::ReadConfigurationFile():: parameters are: "
    << " readyFile: " << mReadyFile.c_str()
    << " machinefile: " << mMachineFile.c_str()
    << " ServerPort:" << mServerPort
    << " persFile: " << mPersistentFilename.c_str()
    << " persLocalFile: " << mPersistentFileLocalPath.c_str()
    << " LocalInfoFile: " << mLocalInfoFile.c_str()
    << " verbsDev" << mCommIF.c_str()
    << EndLogLine;

  return status; 
}

  

skv_config_setting_t
skv_configuration_t::GetVariableCase( const string s, size_t *valueIndex )
{
  skv_config_setting_t setting = SKV_CONFIG_SETTING_UNDEFINED;

  /*******************************************/
  // find out which variable we have to set

  // server variables
  if( s.find( "SKV_SERVER" ) != string::npos )
  {
    if( s.find( "SERVER_PORT" ) != string::npos )
      setting = SKV_CONFIG_SETTING_SERVER_PORT;

    if( s.find( "READY_FILE" ) != string::npos )
      setting = SKV_CONFIG_SETTING_READY_FILE;

    if( s.find( "MACHINE_FILE" ) != string::npos )
      setting = SKV_CONFIG_SETTING_MACHINE_FILE;

    if( s.find( "LOCAL_INFO" ) != string::npos )
      setting = SKV_CONFIG_SETTING_LOCAL_SERVER_INFO_FILE;

    if( s.find( "COMM_IF") != string::npos )
      setting = SKV_CONFIG_SETTING_COMM_IF;
  }
  // client variables
  else if( s.find( "SKV_CLIENT" ) != string::npos )
  {
  }

  // other/general variables
  else
  {
    if( s.find( "PERSISTENT_FILENAME" ) != string::npos )
      setting = SKV_CONFIG_SETTING_PERSISTENT_FILENAME;

    if( s.find( "PERSISTENT_FILE_LOCAL_PATH" ) != string::npos )
      setting = SKV_CONFIG_SETTING_PERSISTENT_FILE_LOCAL_PATH;
  }

  // find the index of the first non-space char of the value after '='
  if( setting != SKV_CONFIG_SETTING_UNDEFINED )
  {
    size_t vPos = s.find( '=' ) + 1;
    while( s[vPos] == ' ' )
    {
      vPos++;
    }
    *valueIndex = vPos;
  }

  return setting;
}

skv_configuration_t*
skv_configuration_t::GetSKVConfiguration( const char *aConfigFile )
{
  if( !mConfiguration )
  {
    mConfiguration = new skv_configuration_t;
    if( mConfiguration->ReadConfigurationFile( aConfigFile ) )
    {
      delete mConfiguration;
      mConfiguration = NULL;
    }
  }
  return mConfiguration;
}

uint32_t
skv_configuration_t::GetSKVServerPort()
{
  return mServerPort;
}

void
skv_configuration_t::SetSKVServerPort( uint32_t aServerPort )
{
  mServerPort = aServerPort;
}

const char*
skv_configuration_t::GetServerReadyFile()
{
  return mReadyFile.c_str();
}

const char*
skv_configuration_t::GetMachineFile()
{
  return mMachineFile.c_str();
}

const char*
skv_configuration_t::GetServerPersistentFilename()
{
  return mPersistentFilename.c_str();
}

const char*
skv_configuration_t::GetServerPersistentFileLocalPath()
{
  return mPersistentFileLocalPath.c_str();
}

const char*
skv_configuration_t::GetServerLocalInfoFile()
{
  return mLocalInfoFile.c_str();
}

const char*
skv_configuration_t::GetCommIF()
{
  return mCommIF.c_str();
}

/*********************************************************************/
// tool routines
skv_config_line_status_t
IsComment( string s )
{
  skv_config_line_status_t line_status;

  line_status = SKV_CONFIG_FILE_EMPTY_LINE;

  for( unsigned int i = 0; i < s.size() && (line_status == SKV_CONFIG_FILE_EMPTY_LINE); i++ )
  {
    switch( s[i] )
    {
      case ' ':               // white spaces don't change any status
      case '\t':
        break;
      case '#':               // # on up to now empty line makes it a comment
        if( line_status == SKV_CONFIG_FILE_EMPTY_LINE )
        {
          line_status = SKV_CONFIG_FILE_COMMENT;
        }
        break;
      default:
        line_status = SKV_CONFIG_FILE_ASSIGNMENT;
    }
  }

  return line_status;
}
