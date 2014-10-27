/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string.h>

#include <FxLogger.hpp>
extern "C" 
{
#define ITAPI_ENABLE_V21_BINDINGS
#include <it_api.h>
};

#define PORT 5553
#define VP_NAME "vp_softrdma"
#define SPIGOT_ID 0

#ifndef LOG_STEPS
#define LOG_STEPS (1)
#endif

#ifndef LOG_IA
#define LOG_IA (1)
#endif

#ifndef LOG_EP
#define LOG_EP (1)
#endif

#ifndef LOG_CONNECT
#define LOG_CONNECT (1)
#endif


typedef enum {
	SERVER_EP = 0,
	CLIENT_EP = 1
} ep_type_t;


it_status_t get_first_interface(it_ia_handle_t *iface)
{
	it_status_t rc = IT_SUCCESS;

	/* create an interface handle from the first entry of the list */
	BegLogLine (LOG_IA)
		<< "creating interface handle:" 
		<< EndLogLine;

	rc = it_ia_create( VP_NAME, 2, 0, iface);
	return rc;

}


it_status_t create_rc_ep(int client, it_ep_handle_t *handle, it_evd_handle_t *connect_sevd_handle, it_evd_handle_t *aevd_handle)
{
	BegLogLine (LOG_EP)
		<< "entering create_rc_ep" 
		<< EndLogLine;

	it_status_t rc = IT_SUCCESS;	/* optimistic approach ;-) */

	it_ia_handle_t iface;
	rc = get_first_interface(&iface);
	if ( rc != IT_SUCCESS ) {
		BegLogLine (1)
			<< "Failed to get an interface\n"
			<< EndLogLine;
		return rc;
	}
			
	BegLogLine (LOG_EP)
		<< "got interface handle:" 
		<< (unsigned long)iface
		<< EndLogLine;

	/* create a protection zone for the end point */
	it_pz_handle_t pzone;
	rc = it_pz_create(iface, &pzone);
	if ( rc != IT_SUCCESS ) {
		fprintf(stderr, "Failed to get an interface\n");

		it_ia_free(iface);
		return rc;
	}
			
	BegLogLine (LOG_EP)
		<< "protection zone created:" 
		<< (unsigned long)pzone
		<< EndLogLine;


	it_evd_handle_t request_sevd_handle;
	size_t ev_queue_size = 5;
	it_evd_flags_t evd_flags = IT_EVD_OVERFLOW_DEFAULT; /* overflow handling described page 84, using default for now */
	it_evd_handle_t aevd_dummy = (it_evd_handle_t)IT_NULL_HANDLE;
	int fd;																							/* unused */
	if ( client == SERVER_EP ) {
		BegLogLine (LOG_EP)
			<< "Creating aevd for server ep:" 
			<< EndLogLine;

		rc = it_evd_create (iface,
												IT_AEVD_NOTIFICATION_EVENT_STREAM,
												evd_flags,
												ev_queue_size,
												0,				// parameter ignored for aevd
												aevd_dummy,
												(it_evd_handle_t*)aevd_handle,
												&fd);
	}
	else {
		BegLogLine (LOG_EP)
			<< "Using dummy aevd for client ep:" 
			<< EndLogLine;
		*aevd_handle = aevd_dummy;
	}

	rc = it_evd_create (iface,
											IT_DTO_EVENT_STREAM,
											evd_flags,
											ev_queue_size,
											1,				/* threshold to trigger aggregated EVD - not used yet */
											*aevd_handle,
											(it_evd_handle_t*)&request_sevd_handle,
											&fd);

	rc = it_evd_create (iface,
											IT_CM_REQ_EVENT_STREAM, /* connect event numbers */
											evd_flags,
											ev_queue_size,
											1,				/* threshold to trigger aggregated EVD - not used yet */
											*aevd_handle,
											(it_evd_handle_t*)connect_sevd_handle,
											&fd);

	it_ep_rc_creation_flags_t ep_flags = IT_EP_NO_FLAG; /* nothing special for now */
	it_ep_attributes_t ep_attr;

	/* general attributes */
	ep_attr.max_dto_payload_size = 1024; /* this will have to be
																					adjusted to the domain,
																					unfortunately it cannot be
																					modified after connection is
																					established :-( */
	ep_attr.max_request_dtos     = 8; /* outstanding communication requests */
	ep_attr.max_recv_dtos        = 32; /* outstanding posted receives (ignored for SRQs) */
	ep_attr.max_send_segments    = 16; /* number data segments for sends */
	ep_attr.max_recv_segments    = 16; /* number data segments for recvs (ignored for SRQs) */

	/* RC specific attributes */
	ep_attr.srv.rc.rdma_read_enable        = IT_TRUE;
	ep_attr.srv.rc.rdma_write_enable       = IT_TRUE; /* enable RDMA on EP */
	ep_attr.srv.rc.max_rdma_read_segments  = 16;
	ep_attr.srv.rc.max_rdma_write_segments = 16; 
	ep_attr.srv.rc.rdma_read_ird           = 32; /* outstanding incoming reads */
	ep_attr.srv.rc.rdma_read_ord           = 8;	 /* outstanding outgoing reads */
	ep_attr.srv.rc.srq                     = (it_srq_handle_t)IT_NULL_HANDLE; /* no SRQ yet */
	ep_attr.srv.rc.soft_hi_watermark       = 0;							 /* disable hi-watermark event generation for now */
	ep_attr.srv.rc.hard_hi_watermark       = 0;

	/* finally create the endpoint for a reliable connection */
	rc = it_ep_rc_create (pzone,
												request_sevd_handle,
												request_sevd_handle, /* possible to use the same EVD for send and recv */
												*connect_sevd_handle,
												ep_flags,
												&ep_attr,
												handle);

	if (rc != IT_SUCCESS) {
		fprintf(stderr, "RC endpoint couldn't be created\n");
	}

	return rc;
}

it_status_t get_path(const char *addr, it_ep_handle_t *ep, it_path_t *path)
{
	it_status_t rc = IT_SUCCESS;
	it_ep_param_t info;
	size_t num_paths;
	size_t total_paths;
	struct hostent *remote;

	it_net_addr_t net_addr;

	net_addr.addr_type = IT_IPV4;
	net_addr.addr.ipv4.s_addr = (*(struct in_addr*)addr).s_addr;
		
	remote = gethostbyname(addr);

	path->u.iwarp.ip_vers = IT_IP_VERS_IPV4;
	path->u.iwarp.laddr.ipv4.s_addr = INADDR_ANY;
	path->u.iwarp.raddr.ipv4.s_addr = *(uint32_t*)remote->h_addr;

	BegLogLine(LOG_CONNECT)
		<< "setting server address to: "
		<< (unsigned char)(remote->h_addr[0]) << "."
		<< (unsigned char)(remote->h_addr[1]) << "."
		<< (unsigned char)(remote->h_addr[2]) << "."
		<< (unsigned char)(remote->h_addr[3]) << "."
		<< EndLogLine;

	return rc;
}


it_status_t client_connect(char *address)
{
	/* IT-API connect */
	it_status_t rc;

	it_ep_handle_t client_ep;
	it_path_t path;
	it_conn_attributes_t conn_attr;
	it_conn_qual_t conn_qual;
	it_cn_est_flags_t flags = IT_CONNECT_FLAG_TWO_WAY;
	it_evd_handle_t connect_sevd_handle;
	it_evd_handle_t event_handler;
	const char *data="0 0";

	BegLogLine (LOG_STEPS)
		<< "Creating client endpoint"
		<< address
		<< EndLogLine;

	rc = create_rc_ep(CLIENT_EP, &client_ep, &connect_sevd_handle, &event_handler);
	BegLogLine (LOG_STEPS)
		<< "created client endpoint: rc = " << rc
		<< " handle = " << client_ep
		<< " cm_hndlr = " << connect_sevd_handle
		<< " ev_hndlr = " << event_handler
		<< EndLogLine;

	if ( rc != IT_SUCCESS ) {
		return rc;
	}


  // rc = it_ep_free(client_ep);
  // BegLogLine( LOG_STEPS )
  //   << "endpoint destroyed"
  //   << EndLogLine;

  // return rc;


// 	path = (it_path_t*)malloc(sizeof(it_path_t));
// 	rc = get_path(address, &client_ep, path);
// 	if ( rc != IT_SUCCESS ) {
// 		return rc;
// 	}

	struct hostent *remote;

	remote = gethostbyname(address);

	BegLogLine(LOG_CONNECT)
		<< "setting server address to: "
		<< (unsigned char)(remote->h_addr[0]) << "."
		<< (unsigned char)(remote->h_addr[1]) << "."
		<< (unsigned char)(remote->h_addr[2]) << "."
		<< (unsigned char)(remote->h_addr[3]) << "."
		<< EndLogLine;

	path.u.iwarp.ip_vers = IT_IP_VERS_IPV4;
	path.u.iwarp.laddr.ipv4.s_addr = INADDR_ANY;
	path.u.iwarp.raddr.ipv4.s_addr = *(uint32_t*)remote->h_addr;

	BegLogLine (LOG_STEPS)
		<< "retrieved path to: " << address
		<< " rc = " << rc
		<< " path = " << path.u.iwarp.raddr.ipv4.s_addr
		<< EndLogLine;

	conn_qual.type = IT_IANA_LR_PORT;
	conn_qual.conn_qual.lr_port.local  = 0;
	conn_qual.conn_qual.lr_port.remote = htons(PORT);
	conn_attr.iwarp.unused = NULL;

	rc = it_ep_connect(client_ep,
										 &path,
										 &conn_attr,
										 &conn_qual,
										 flags,
										 (const unsigned char *)data,
										 strlen(data));

	BegLogLine (LOG_STEPS)
		<< "Connect returned: "
		<< rc
		<< EndLogLine;

  if( rc != IT_SUCCESS )
    return rc;


  sleep(2);
  it_ep_disconnect(client_ep,
                   NULL, 0);


	BegLogLine (LOG_STEPS)
		<< "disconnect returned: "
		<< rc
		<< EndLogLine;

  

	return rc;
}

// void get_server_address(char **addr, size_t *len)
// {
// 	struct sockaddr_in *new_addr;
// 	new_addr = (struct sockaddr_in*)malloc(sizeof(struct sockaddr_in));
// 	*len = sizeof(struct sockaddr_in);
	
// 	memset(new_addr, 0, *len);
// 	memcpy(new_addr, *addr, MAX(*len, strlen(addr)) );
	

// 	*addr = (char*)new_addr;
// }


int run_client(char *address)
{
	BegLogLine(LOG_STEPS) << address << EndLogLine ;
	size_t addrlen;

// 	get_server_address(&address, &addrlen);

	client_connect(address);
	
	BegLogLine (LOG_STEPS)
		<< "Finishing client"
		<< EndLogLine;

	return 0;
}



int server_connect(char *address)
{
	/* IT-API listen, accept */
	it_ep_handle_t server_ep;
	it_status_t rc = IT_SUCCESS;
	it_ia_handle_t ia;
	it_evd_handle_t connect_sevd_handle;
	it_evd_handle_t event_handler;

	BegLogLine(LOG_STEPS) << address << EndLogLine ;

	BegLogLine (LOG_STEPS)
		<< "Creating interface"
		<< EndLogLine;

	get_first_interface(&ia);
	
	BegLogLine (LOG_STEPS)
		<< "Creating server endpoint"
		<< EndLogLine;

	/* create server endpoint */
	rc = create_rc_ep(SERVER_EP, &server_ep, &connect_sevd_handle, &event_handler);
	if ( rc != IT_SUCCESS ) {
		fprintf(stderr, "Failed to create server endpoint\n");
		return rc;
	}

	/* set the qualifier to hold the listen port */
	it_conn_qual_t conn_qual;
	conn_qual.type = IT_IANA_PORT;
	conn_qual.conn_qual.lr_port.local = htons(PORT);
	conn_qual.conn_qual.lr_port.remote = 0;

	BegLogLine (LOG_STEPS)
		<< "Creating listener"
		<< EndLogLine;

	/* create a listen point */
	it_listen_handle_t listen_handle;
	it_listen_flags_t listen_flags = IT_LISTEN_CONN_QUAL_INPUT;
	it_listen_create(ia,
									 SPIGOT_ID,
									 connect_sevd_handle,
									 listen_flags,
									 &conn_qual,
									 &listen_handle);

	/* event handling/dispatch/schedule/wait */
	it_event_t conn_event;
	size_t timeout = 60000000;				/* wait for 1 min */
	size_t got_some;

	BegLogLine (LOG_STEPS)
		<< "Waiting for incoming connection..."
		<< EndLogLine;

	rc = itx_aevd_wait(event_handler,
										 0,
										 1, // PIMD_SERVER_AEVD_EVENTS_MAX_COUNT,
										 &conn_event,
										 &got_some );

	BegLogLine (LOG_STEPS)
		<< "Received event: rc = "
		<< rc
		<< EndLogLine;


	if ( rc != IT_SUCCESS) {
		fprintf(stderr, "Failed to retrieve event from dispatcher\n");
		return rc;
	}
	if (conn_event.event_number != IT_CM_MSG_CONN_ESTABLISHED_EVENT) {
		fprintf(stderr, "got unexpected event\n");
		return rc;
	}

	BegLogLine (LOG_STEPS)
		<< "Accepting connection..."
		<< EndLogLine;

	rc = it_ep_accept(server_ep,
										conn_event.conn.cn_est_id,
										(const unsigned char *)"0 0", 3);
	if ( rc != IT_SUCCESS ) {
		fprintf(stderr, "Failed accepting endpoint\n");
		return rc;
	}

	BegLogLine (LOG_STEPS)
		<< "Accepted connection: rc = "
		<< rc
		<< EndLogLine;

  if( rc != IT_SUCCESS )
    {
      return rc;
    }

  sleep(2);
  sleep(2000) ;
  it_ep_disconnect(server_ep,
                   NULL, 0);

	BegLogLine (LOG_STEPS)
		<< "Accepted connection: rc = "
		<< rc
		<< EndLogLine;


	return rc;
}


int run_server(char *address)
{

	BegLogLine (LOG_STEPS) << "Server connecting" << address << EndLogLine;
	server_connect(address);

	BegLogLine (LOG_STEPS) << "Finishing server" << EndLogLine; 

	return 0;
}



// run as: connect_it -<c|s> <server address>
int main(int argc, char **argv)
{
	if (argc < 2) {
		printf("mode -<server|client (s|c)> missing\n");
		return -1;
	}

  FxLogger_Init( argv[ 0 ] );
  BegLogLine(1)
    << "Starting program..."
    << EndLogLine;

	if ((argv[1])[1] == 'c') {
		return run_client(argv[2]);
	}
	else {
		return run_server(argv[2]);
	}


  BegLogLine(1)
    << "Finishing program..."
    << EndLogLine;
	return 0;
}
