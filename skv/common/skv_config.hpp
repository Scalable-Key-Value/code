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
#define DEFAULT_SKV_FORWARDER_PORT 10950
#define DEFAULT_SKV_SERVER_READY_FILE "/tmp/skv_server.ready"
#define DEFAULT_SKV_MACHINE_FILE "/etc/skv_machinefile"
#define DEFAULT_SKV_PERSISTENT_FILENAME "skv_store"
#define DEFAULT_SKV_PERSISTENT_FILE_LOCAL_PATH "/tmp/skv_store"
#define DEFAULT_SKV_COMM_IF "roq0"
#define DEFAULT_SKV_RDMA_MEMORY_LIMIT ( 2 * 1024 * 1024 * 1024 )

typedef enum {
  SKV_CONFIG_SETTING_UNDEFINED,
  SKV_CONFIG_SETTING_SERVER_PORT,
  SKV_CONFIG_SETTING_FORWARDER_PORT,
  SKV_CONFIG_SETTING_READY_FILE,
  SKV_CONFIG_SETTING_MACHINE_FILE,
  SKV_CONFIG_SETTING_PERSISTENT_FILENAME,
  SKV_CONFIG_SETTING_PERSISTENT_FILE_LOCAL_PATH,
  SKV_CONFIG_SETTING_COMM_IF,
  SKV_CONFIG_SETTING_RDMA_MEMORY_LIMIT
} skv_config_setting_t;



class skv_configuration_t
{
  static skv_configuration_t *mConfiguration;

  uint32_t  mServerPort;   // stored in host byte order
  uint32_t  mForwarderPort;   // stored in host byte order
  string    mReadyFile;
  string    mMachineFile;
  string    mPersistentFilename;
  string    mPersistentFileLocalPath;
  string    mCommIF;
  uint64_t  mRdmaMemoryLimit;

  string    mConfigFile;

  skv_configuration_t();
  int GetConfigurationFile( const char *aConfigFile = NULL );
  skv_config_setting_t GetVariableCase( const string s, size_t *valueIndex );
  int ReadConfigurationFile( const char *aConfigFile = NULL );

public:
  static skv_configuration_t* GetSKVConfiguration( const char* aConfigFile = NULL );

  const char* GetServerReadyFile() const;

  const char* GetMachineFile() const;

  const char* GetServerPersistentFilename() const;
  const char* GetServerPersistentFileLocalPath() const;

  const uint32_t GetSKVServerPort() const;
  const uint32_t GetSKVForwarderPort() const;

  // input in network byte order
  void SetSKVServerPort( uint32_t aServerPort );

  const char* GetCommIF() const;

  const uint64_t GetRdmaMemoryLimit() const;

  const string GetConfigFileName() const;
};

#endif // __SKV_CONFIG_HPP__
