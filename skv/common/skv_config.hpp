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

#ifndef __SKV_CONFIG_HPP__
#define __SKV_CONFIG_HPP__

#include <string>
using namespace std;            // that's a bad place for using... ;-)...

#define DEFAULT_SKV_CONFIG_FILE_NAME "skv_server.conf"

#ifndef DEFAULT_CONFIG_FILE
#define DEFAULT_CONFIG_FILE "/etc/skv_server.conf"
#endif

#define DEFAULT_SKV_SERVER_PORT 17002
#define DEFAULT_SKV_SERVER_READY_FILE "/tmp/skv_server.ready"
#define DEFAULT_SKV_MACHINE_FILE "/etc/machinefile"
#define DEFAULT_SKV_PERSISTENT_FILENAME "beef_slab"
#define DEFAULT_SKV_PERSISTENT_FILE_LOCAL_PATH "/tmp/beef_slab"
#define DEFAULT_SKV_LOCAL_SERVER_INFO_FILE "/tmp/skv_server_info"
#define DEFAULT_SKV_COMM_IF "roq0"

typedef enum {
  SKV_CONFIG_SETTING_UNDEFINED,
  SKV_CONFIG_SETTING_SERVER_PORT,
  SKV_CONFIG_SETTING_READY_FILE,
  SKV_CONFIG_SETTING_MACHINE_FILE,
  SKV_CONFIG_SETTING_PERSISTENT_FILENAME,
  SKV_CONFIG_SETTING_PERSISTENT_FILE_LOCAL_PATH,
  SKV_CONFIG_SETTING_LOCAL_SERVER_INFO_FILE,
  SKV_CONFIG_SETTING_COMM_IF,
} skv_config_setting_t;



class skv_configuration_t
{
  static skv_configuration_t *mConfiguration;

  uint32_t  mServerPort;   // stored in host byte order
  string    mReadyFile;
  string    mMachineFile;
  string    mPersistentFilename;
  string    mPersistentFileLocalPath;
  string    mLocalInfoFile;
  string    mCommIF;

  string    mConfigFile;

  skv_configuration_t();
  int GetConfigurationFile( const char *aConfigFile = NULL );
  skv_config_setting_t GetVariableCase( const string s, size_t *valueIndex );
  int ReadConfigurationFile( const char *aConfigFile = NULL );

public:
  static skv_configuration_t* GetSKVConfiguration( const char* aConfigFile = NULL );

  const char* GetServerReadyFile();

  const char* GetMachineFile();

  const char* GetServerPersistentFilename();
  const char* GetServerPersistentFileLocalPath();

  uint32_t GetSKVServerPort();
  // input in network byte order
  void SetSKVServerPort( uint32_t aServerPort );

  const char* GetServerLocalInfoFile();
  const char* GetCommIF();

};

#endif // __SKV_CONFIG_HPP__
