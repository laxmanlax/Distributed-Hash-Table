In this project, we are designing a distributed hash table over 8 peers. The peers will serve as clients as well as servers. When the peer starts, it will read a global configuration file to read the IP: PORT combination where it should listen as a server. 

There would be following two threads:
1.1.1	Server thread

This thread will maintain a hash table of 1M entries. The hash table for this assignment is a simple array of 1M.

•	Server will run a select() linux call to listen to the current open client fd’s.  
•	Clients will establish socket connection to a peer server will be established only once i.e. during the first request. After that, the socket will be kept alive till the time peer/server shuts down.


1.1.2	Client thread 

This thread will present the users with following options:
1.	*** Enter 1 for inserting an entry *** → PUT
2.	*** Enter 2 for getting an entry *** → GET
3.	*** Enter 3 for deleting an entry *** → DEL
4.	*** Enter 4 to exit *** → EXIT

Along with sending the request to the primary server, these API’s would send a request to the replica as well to insert, delete or get the entry. 

In case of insertion or deletion, the same entry would be inserted or deleted respectively in the replica as well.

While retrieving, primary server is down, secondary server would be contacted. An error would be returned if the secondary server could not be reached as well. 

