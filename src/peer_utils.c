#include "peer_utils.h"

#define EOK 0

/* Global structure to save the replica's config's */
char *replica_servers[NUM_SERVERS][2];

/* FD's for primary and secondary replica's 
 * First column will have primary's fd and
 * second column will have secondary's fd
 */
int server_fds[NUM_SERVERS][1][2];

/* Lock for all hash operations */
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

/* Populate the server data from the config file */
int populate_servers(char *servers[NUM_SERVERS][2]) 
{
	FILE *fp;
	char *temp = NULL, line[80];
	int i=0;
	fp = fopen("server_config", "r");
	if (fp < 0) {
		printf("Could not populate the servers\n");
		return -1;
	}
	while (fgets(line, 80, fp) != NULL && i < NUM_SERVERS) {
		temp = strchr(line, ' ');
		servers[i][0] = calloc(1, strlen(line)-strlen(temp)+1);
		servers[i][1] = calloc(1, strlen(temp));
		if (!servers[i][0] || !servers[i][1]) {
			printf("Could not allocate memory for populating servers\n");
			fclose(fp);
			return -1;
		}
		strncpy(servers[i][0], line, strlen(line) - strlen(temp));
		if (!servers[i][0]) {
			printf("Could not allocate memory for populating server ip\n");
			fclose(fp);
			return -1;
		}
		strncpy(servers[i][1], temp+1, strlen(temp));
		/* Just in case strncpy fails */
		if (!servers[i][1]) {
			printf("Could not allocate memory for populating server port\n");
			fclose(fp);
			return -1;
		}
		i++;
	}
	fclose(fp);
	/* Now populate replica servers */
	populate_replica_servers();
	return 0;
}

/* Populate the server data from the config file */
int populate_replica_servers(void) 
{
	FILE *fp;
	char *temp = NULL, line[80];
	int i=0;
	fp = fopen("replica_server_config", "r");
	if (fp < 0) {
		printf("Could not populate the servers\n");
		return -1;
	}
	while (fgets(line, 80, fp) != NULL && i < NUM_SERVERS) {
		temp = strchr(line, ' ');
		replica_servers[i][0] = calloc(1, strlen(line)-strlen(temp)+1);
		replica_servers[i][1] = calloc(1, strlen(temp));
		if (!replica_servers[i][0] || !replica_servers[i][1]) {
			printf("Could not allocate memory for populating servers\n");
			fclose(fp);
			return -1;
		}
		strncpy(replica_servers[i][0], line, strlen(line) - strlen(temp));
		if (!replica_servers[i][0]) {
			printf("Could not allocate memory for populating server ip\n");
			fclose(fp);
			return -1;
		}
		strncpy(replica_servers[i][1], temp+1, strlen(temp));
		/* Just in case strncpy fails */
		if (!replica_servers[i][1]) {
			printf("Could not allocate memory for populating server port\n");
			fclose(fp);
			return -1;
		}
		i++;
	}
	fclose(fp);
	return 0;
}

/* Internal unction to insert the hash entry */
static void insert_hash_entry (char *hash_table[12500][1], char *key, char *value) 
{
	/* Calculate the index at which entry should go */
	int hash = server_compute_hash(key);
	/* Take a lock on the hash map */
	pthread_mutex_lock(&lock);
	/* Copy the value */
	hash_table[hash][0] = value;
	/* Release the lock */
	pthread_mutex_unlock(&lock);
	printf("Successfully inserted key \n");
}

/* Internal function to get the hash entry */
static char* get_hash_entry (char *hash_table[12500][1], char *key) 
{
	int hash = server_compute_hash(key);		
	printf("Key is %s", hash_table[hash][0]);
	return hash_table[hash][0];
}

/* Internal function to delete the hash entry */
static int delete_hash_entry (char *hash_table[12500][1], char *key) 
{
	int hash = server_compute_hash(key);		
	if (hash_table[hash][0] != NULL) {
		/* Take a lock on the hash map */
		pthread_mutex_lock(&lock);
		free(hash_table[hash][0]);
		hash_table[hash][0] = NULL;
		/* Release the lock */
		pthread_mutex_unlock(&lock);
	} else {
		return -1;
	}
	return 0;
}

/* Execute PUT/GET/DELETE in a separate thread */
void *execute_oper(void *data) 
{
	struct server_data *serv_data = (struct server_data *) data;
	char *msg = serv_data->msg;
	char *temp = NULL;
	char *key = NULL;
	char *value = NULL;
	int res = 0;
	if (strstr(serv_data->msg, "put")) {
		temp = strchr(serv_data->msg, ':');

		key = (char *)calloc(1, strlen(msg) - strlen(temp) - 3);
		value = (char *)calloc(1, strlen(temp));
		if (!key || !value) {
			printf("Could not allocate memory for key/value pair\n");
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}

		strncpy(key, serv_data->msg + 4, strlen(msg) - strlen(temp) - 4);
		strncpy(value, temp+1, strlen(temp) - 1);
		if (!key || !value) {
			printf("Could not populate key/value pair\n");
			if (key) free(key);
			if (value) free(value);
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}

		printf("Inserting key %s value %s\n", key, value);
		if (serv_data) {
			insert_hash_entry(serv_data->hash_table, key, value);
			send(serv_data->client_fd, "Success", strlen("Success"), 0);
		} else {
			printf("server data isnt initialized properly");
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
		}

	} else if (strstr(serv_data->msg, "get")) {

		temp = strchr(serv_data->msg, ',');
		key = (char *)calloc(1, strlen(msg)-3);
		if (!key) {
			printf("Could not allocate memory for key\n");
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}
		strncpy(key, serv_data->msg + 4, strlen(serv_data->msg) - 4);
		if (!key) {
			printf("Could not populate key\n");
			if (key) free(key);
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}
		printf("Searching key %s\n", key);
		value = get_hash_entry(serv_data->hash_table, key);
		if (key) free(key);
		if (value != NULL)  {
			printf("Sending value %s",value);
			send(serv_data->client_fd, value, strlen(value), 0);
		} else {
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
		}

	} else if (strstr(serv_data->msg, "del")) {

		temp = strchr(serv_data->msg, ',');
		key = (char *)calloc(1, strlen(msg)-3);
		if (!key) {
			printf("Could not allocate memory for key\n");
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}
		strncpy(key, serv_data->msg + 4, strlen(serv_data->msg) - 4);
		if (!key) {
			printf("Could not populate key\n");
			if (key) free(key);
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}
		printf("Searching key %s\n", key);
		res = delete_hash_entry(serv_data->hash_table, key);
		if (res == EOK) {
			send(serv_data->client_fd, "Success", strlen("Sucess"), 0);
		} else {
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
		}
	} else {
		printf("Undefined operation\n");
		send(serv_data->client_fd, "Undefined", strlen("Undefined"), 0);
	}
	return NULL;
}

/* Find the server to which request should be sent */
int get_hashing_server (char *key) {
	int hash = 0;
	char *temp = strdup(key);
	while (*temp != '\0') {
		hash = hash + *temp;
		temp++;
	}
	temp = NULL;
	return hash % NUM_SERVERS;
}

/* Function to get the value for a key from server */
char* get (char *key, char *servers[NUM_SERVERS][2]) 
{ 
	int flag = -1, server_num = -1, server_fd = -1;
	struct sockaddr_in serv_addr;
	struct sockaddr_storage server_str;
	socklen_t server_addr_size;
	char buffer_send[MAX_MSG_SIZE] = {0};
	char *buffer_recv = (char *)calloc(1, MAX_MSG_SIZE);

	if (buffer_recv == NULL) {
		printf("Could not allocate memory for receiving data\n");
		return NULL;
	}

	server_num = get_hashing_server (key) % NUM_SERVERS;

	printf ("Looking up Socket info for Server %d\n", server_num);
	printf ("Sending lookup request to %s:%s\n", servers[server_num][0], 
			servers[server_num][1]);

	if (server_fds[server_num][0][0] <= 0) {
		server_fds[server_num][0][0] = socket(PF_INET, SOCK_STREAM, 0);
		if (server_fds[server_num][0][0] < 0) {
			printf("Could not create the socket\n");
			goto get_sec;
		}
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][0], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
			printf("Couldnt bind to the socket \n");
			goto get_sec;
		}
	}
		sprintf(buffer_send, "get,%s", key);
		flag = send(server_fds[server_num][0][0], buffer_send, MAX_MSG_SIZE, 0);
		if (flag < 0) {
			printf("Couldnt send message to server \n");
			goto get_sec;
		}
		flag = recv(server_fds[server_num][0][0], buffer_recv, MAX_MSG_SIZE, 0);
		if (flag < 0) {
			printf("Couldnt receive message from server \n");
			goto get_sec;
		}
		return buffer_recv;
get_sec:
	printf("Looking up information from secondary server\n");
	if (server_fds[server_num][0][1] <= 0) {
		server_fds[server_num][0][1] = socket(PF_INET, SOCK_STREAM, 0);
		if (server_fds[server_num][0][1] < 0) {
			printf("Could not create the socket\n");
			return NULL;
		}
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][1], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
			printf("Couldnt bind to the socket \n");
			return NULL;
		}
	}
	sprintf(buffer_send, "get,%s", key);
	printf("Sending lookup request %s to replica\n", buffer_send);
	flag = send(server_fds[server_num][0][1], buffer_send, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("Couldn't send message to secondary server\n");
		return NULL;
	}
	flag = recv(server_fds[server_num][0][1], buffer_recv, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("Couldnt receive message from secondary server \n");
		return NULL;
	}
	return buffer_recv;
}

char *del (char *key,  char *servers[NUM_SERVERS][2]) 
{ 
	int flag = -1, server_num = -1, server_fd = -1;
	struct sockaddr_in serv_addr;
	struct sockaddr_storage server_str;
	socklen_t server_addr_size;
	char buffer_send[MAX_MSG_SIZE] = {0};
	char buffer_recv[MAX_MSG_SIZE] = {0};
	char *result = NULL;

	server_num = get_hashing_server (key) % NUM_SERVERS;

	printf ("Looking up Socket info for Server %d\n", server_num);
	printf ("Sending delete request to %s:%s\n", servers[server_num][0], 
			servers[server_num][1]);

	if (server_fds[server_num][0][0] <= 0) {
		server_fds[server_num][0][0] = socket(PF_INET, SOCK_STREAM, 0);
		if (server_fds[server_num][0][0] < 0) {
			printf("Could not create the socket\n");
			goto del_sec;
		}
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][0], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
			printf("Couldnt bind to the socket \n");
			goto del_sec;
		}
	}

	sprintf(buffer_send, "del,%s", key);
	flag = send(server_fds[server_num][0][0], buffer_send, MAX_MSG_SIZE, 0);

	if (flag < 0) {
		printf("Couldnt send message to server \n");
			goto del_sec;
	}

	flag = recv(server_fds[server_num][0][0], buffer_recv, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("Couldnt receive message from server \n");
			goto del_sec;
	}
del_sec:
	if (server_fds[server_num][0][1] <= 0) {
		server_fds[server_num][0][1] = socket(PF_INET, SOCK_STREAM, 0);
		if (server_fds[server_num][0][1] < 0) {
			printf("Could not create the socket\n");
			return NULL;
		}
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][1], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
			printf("Couldnt bind to the socket \n");
			return NULL;
		}
	}
	printf("Sending delete request to replica \n");
	flag = send(server_fds[server_num][0][1], buffer_send, MAX_MSG_SIZE, 0);

	if (flag < 0) {
		printf("Couldnt send message to server \n");
		return NULL;
	}

	//flag = recv(server_fds[server_num][0][1], buffer_recv, MAX_MSG_SIZE, 0);
	//if (flag < 0) {
	//	printf("Couldnt receive message from replica server \n");
	//	return NULL;
	//}
	//result = strdup(buffer_recv);
	return result;
}

char *put (char *key ,char *value, char *servers[NUM_SERVERS][2]) 
{ 
	int flag = -1, server_num = -1, server_fd = -1;
	struct sockaddr_in serv_addr;
	struct sockaddr_storage server_str;
	socklen_t server_addr_size;
	char buffer_send[MAX_MSG_SIZE] = {0};
	char buffer_recv[MAX_MSG_SIZE] = {0};
	char *result = NULL;

	server_num = get_hashing_server (key) % NUM_SERVERS;

	printf ("Looking up Socket info for Server %d\n", server_num);
	printf ("Sending put request to %s:%s\n", servers[server_num][0], 
			servers[server_num][1]);

	/* Create a socket connection with primary server if none exists */
	if (server_fds[server_num][0][0] <= 0) {
		/* Bind to the correct server and send a put request */
		server_fds[server_num][0][0] = socket(PF_INET, SOCK_STREAM, 0);
		if (server_fds[server_num][0][0] < 0) {
			printf("Could not create the socket\n");
			goto put_sec;
		}
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][0], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
			printf("Couldnt bind to the socket \n");
			goto put_sec;
		}
	}
	sprintf(buffer_send, "put,%s:%s", key, value);
	flag = send(server_fds[server_num][0][0], buffer_send, MAX_MSG_SIZE, 0);

	if (flag < 0) {
		printf("Couldnt send message to server \n");
		goto put_sec;
	}

	flag = recv(server_fds[server_num][0][0], buffer_recv, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("Couldnt receive message from server \n");
		goto put_sec;
	}

put_sec:
	printf ("Sending put request to replica server %s:%s\n", replica_servers[server_num][0], 
			replica_servers[server_num][1]);

	/* Create a socket connection with secondary server if none exists */
	if (server_fds[server_num][0][1] <= 0) {
		server_fds[server_num][0][1] = socket(PF_INET, SOCK_STREAM, 0);
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(replica_servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(replica_servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][1], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
			printf("Couldnt bind to the socket \n");
			return NULL;
		}
	}

	sprintf(buffer_send, "put,%s:%s", key, value);
	flag = send(server_fds[server_num][0][1], buffer_send, MAX_MSG_SIZE, 0);

	if (flag < 0) {
		printf("Couldnt send message to server \n");
		return NULL;
	}
	flag = recv(server_fds[server_num][0][1], buffer_recv, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("Couldnt receive message from server \n");
		return NULL;
	}
	result = strdup(buffer_recv);
	return result;
}

int server_compute_hash(char *key) {
	int hash = 0;
	char *temp = strdup(key);
	while (*temp != '\0') {
		hash = hash + *temp;
		temp++;
	}
	temp = NULL;
	return hash % MAX_HASH_ENTRIES;	
}

/* Start a server for myself */
void *server (char *ip, char *port, char *hash_table[12500][1]) {
	int server_fd, client_fd, flag, maxfd, i;
	char buff[MAX_MSG_SIZE];
	struct sockaddr_in serv_addr;
	struct sockaddr_storage server_str;
	socklen_t server_addr_size;
	fd_set readset;

	server_fd = socket(PF_INET, SOCK_STREAM, 0);
	if (server_fd < 0) {
		printf("Couldnt create the socket \n");
		goto exit;
	}
	serv_addr.sin_family = AF_INET;
	printf("Binding at ip %s port %d\n", ip, atoi(port));
	serv_addr.sin_port = htons(atoi(port));
	serv_addr.sin_addr.s_addr = inet_addr(ip);
	memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

	flag = bind(server_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr));
	if (flag == -1) {
		printf("Couldnt bind to the socket \n");
		goto exit;
	}
	printf("Listening");
	flag = listen(server_fd, MAX_CONN_LISTEN);

	if (flag != 0) {
		printf("Error listening to the connection\n");
		goto exit;
	}
	server_addr_size = sizeof(server_str);

	/* Initialize the readset to monitor this server_fd */
	FD_ZERO(&readset);
	FD_SET(server_fd, &readset);
	maxfd = server_fd;
	int ret = 0;
	fd_set tempset;
	printf("max fd %d", maxfd);
	do {
		memcpy(&tempset, &readset, sizeof(tempset));
		/* Run select call for messages for this fd */
		ret = select(maxfd + 1, &tempset, NULL, NULL, NULL);
		if (ret < 0) {
			printf("Error in select()\n");
		} else if (ret > 0) {
			/* We have a valid message, set this in server's readset */
			if (FD_ISSET(server_fd, &tempset)) {
				printf("Accepting connection \n");
				client_fd = accept(server_fd, (struct sockaddr *) &server_str, &server_addr_size);
				if (client_fd < 0) {
					printf("Error in accept(): \n");
				} else {
					/* Set the client_fd in readset */
					printf("Setting client_fd %d", client_fd);
					FD_SET(client_fd, &readset);
					if (client_fd > maxfd) {
						maxfd = client_fd;
					}
				}
			}

			/* Iterate over all fd's in the readset */
			for (i=0; i<maxfd+1; i++) {
				/* If we have a message for i'th client in the read set, read it */
				if (FD_ISSET(i, &tempset)) {
					pthread_t thread;
					ret = recv (i, buff, MAX_MSG_SIZE, 0);
					if (ret >0 ) {
						printf("Received message new from client %s %d\n", buff, ret);
						/* Initialise the structure to spawn this request as a thread */
						struct server_data *data = (struct server_data *)calloc(1, sizeof(struct server_data));
						data->msg = (char *)calloc(1, strlen(buff));
						strncpy(data->msg, buff, strlen(buff));
						data->hash_table = hash_table;
						data->client_fd = client_fd;

						/* Create a thread for this operation */
						pthread_create (&thread, NULL, execute_oper, (void *)data); 
						printf("Thread done.. join\n");
						pthread_join(thread, NULL);
					} else {
						close(i);
						FD_CLR(i, &readset);
					}
				}
			}

		}
	} while(1);

exit:
	/* Destroy any locks for this server */
	pthread_mutex_destroy(&lock);

	return 0;
}


