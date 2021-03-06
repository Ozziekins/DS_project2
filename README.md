# DS Project2 - Python Implementation of Distributed File System  



## Project Description  

Implementing a simple Distributed File System (DFS) using python as our programming language.  

The Distributed File System (DFS) is a file system with data stored on a server. The data is accessed and processed as if it was stored on the local client machine. The DFS makes it convenient to share information and files among users on a network.  



## Basic functions supported by our DFS  

**Client side**  

- Initialize:
  - Initialize the client storage on a new system, should remove any existing file in the dfs root directory and return available size.

- File create:
  - Allows creation of a new empty file.

- File read:
  - Allows reading any file from DFS (download a file from the DFS to the Client side).
- File write:
  - Allows putting any file to DFS (upload a file from the Client side to the DFS)

- File delete:
  - Allows deleting any file from DFS

- File info:
  -  Provides information about the file (any useful information - size, node id, etc.)

- File copy:
  - Allows creating a copy of file.

- File move:
  - Allows moving a file to the specified path.

- Open directory:
  - Allows changing directory

- Read directory:
  - Returns list of files, which are stored in the directory.

- Make directory:
  - Allows creation of a new directory.

- Delete directory:
  - Allows deletion of a directory.  If the directory contains files the system asks for confirmation from the user before deletion.  



**Storage Server**

- Replication:
  - Files will be replicated on multiple storage servers.

- Directory management:
  - Accesses files using ```DATA_DIR + file_name``` where ```DATA_DIR``` is ```\tmp\storage``` and ```n``` is the storage server number

- Handles client requests  



**Naming Server**  

- File striping:
  -  Slices a file into several chunks or blocks; and our ```BLOCK_SIZE``` is **128**  



## Prerequisites  

- About 5 EC2 instance running and a lot of money to support them 😭  

- Instance ip's:  
  - naming_server: 3.23.185.197  
  - storage_server1: 3.23.149.225  
  - storage_server2: 3.23.228.220  
  - storage_server3: 52.15.190.62

- DockerHub account  

- DockerHub images:
  - ozziekins/client  
  - ozziekins/naming  
  - ozziekins/storage  
  

## How to Launch  

<u>Step 1</u>: Launch the amazon instances  

<u>Step 2</u>: ssh into the instances using the command in  the connect tab  

<u>Step 3</u>: Pull our docker images on the various instances using the command in step 4  

<u>Step 4</u>: ```docker pull <image_name>```  

<u>Step 5</u>: Make use of any of the commands listed below on the instance hosting the client  



## Commands

```

$: init

$: create <file_name>

$: read <file_name>

$: write <src_file> <dest_file>

$: delete <file_name>

$: info <file_name>

$: copy <file_name>  

$: root

$: move src dest

$: cd  directory

$: ls directory

$: mkdir directory

$: dltdir directory
```



## Architectural Diagrams  

**Overall architecture**  



![alt text](https://github.com/Ozziekins/DS_project2/blob/master/images/diagram1.png?raw=true)  



**Naming server and file system**  



![alt text](https://github.com/Ozziekins/DS_project2/blob/master/images/diagram2.png?raw=true)  



**Storage servers and replication**  



![alt text](https://github.com/Ozziekins/DS_project2/blob/master/images/diagram3.png?raw=true)



## Technology Stacks  

![alt text](https://github.com/Ozziekins/DS_project2/blob/master/images/stacks.png?raw=true)  



## Communication Protocols  

Our system uses an iterative manner of communicating where the client first goes to the naming server to get the required metadata, such as file chunk id and the particular storage server that the chunk can be found on.  

Then the client takes this information and goes to the required storage server to carry out whatever command the client needs.  



For communication, the various parts of our system make use of **rpyc** (remote python call) to communicate with each other. Hence, each component has it's specified ip address and port. 



## Team and Contributions  

**Team name:**   

DFST  

**Members:**  

Ozioma Okonicha (o.okonicha@innopolis.university)👩🏾‍💻🇳🇬  

Anastassiya Ryabkova (a.ryabkova@innopolis.university)👩🏼‍💻🇰🇿  

Daniel Atonge (d.atonge@innopolis.university)🧑🏿‍💻🇨🇲  



**Contributions proven by Trello**  

![alt text](https://github.com/Ozziekins/DS_project2/blob/master/images/trello.png?raw=true)

**Contributions proven by Github**  

![alt text](https://github.com/Ozziekins/DS_project2/blob/master/images/github.png?raw=true)
