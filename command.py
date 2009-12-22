#! /usr/bin/env python

# IP adress of the master
host = '127.0.0.1'
port = 47000

import socket, sys, time, os

usage="""
Usage :

SUBMIT  <Filename>  :  Submit a list of jobs - Give absolute pathfile 
INFO                :  Get status of nodes
LISTJOB             :  Print job list and status
DELJOB              :  Delete a job
RESERVE   <Node>    :  Reserve a node for threaded process

START   <Filename>  :  Start the cluster - Give a list of nodes
HALT                :  Stop the cluster
STARTNODE <Node>    :  Start a node
HALTNODE  <Node>    :  Stop a node

"""

if len(sys.argv)==1:
    print usage
    sys.exit()

# Function to connect to the server
def connect():

    connexion = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        connexion.connect((host, port))
    except socket.error:
        #print "Connexion failed. Quit!"
	return 'NULL'
        #sys.exit()

    print "Connected to the master."
    return connexion

# COMMAND to submit new job
if sys.argv[1]=='SUBMIT':
    if len(sys.argv)<3:
        print "Argument missing."
	sys.exit()

    connexion = connect()
    if connexion=='NULL':
        print 'No server found'
        sys.exit()

    msg_send="SUBMIT %s"%(sys.argv[2])
    print msg_send
    connexion.send(msg_send)
    msg_rcv=connexion.recv(1024)
    print "%s jobs submitted"%msg_rcv
    connexion.close()

# Get cluster info
elif sys.argv[1]=='INFO':

    connexion = connect()
    if connexion=='NULL':
        print 'No server found'
        sys.exit()

    msg_send="INFO"
    connexion.send(msg_send)
    msg_rcv=connexion.recv(1024) # msg = "OK nb_nodes"
    nb_nodes = int( msg_rcv.split()[1] )
    for i in range(nb_nodes):
        msg_rcv=connexion.recv(1024) # msg = "node_name nb_cores"
	# Print reserved Node
	if (msg_rcv.split()[2+int(msg_rcv.split()[1])])=="1":
	  print "node : %s  L"%msg_rcv.split()[0]
        else:  # Noeud non reserve
	  print "node : %s"%msg_rcv.split()[0]
	for j in range(int(msg_rcv.split()[1])):
	    print "  CPU #%d : %s"%(j,msg_rcv.split()[2+j])
	print ""
    connexion.close()

# Get Job infos
elif sys.argv[1]=='LISTJOB':

    connexion = connect()
    if connexion=='NULL':
        print 'No server found'
        sys.exit()

    msg_send="LISTJOB"
    connexion.send(msg_send)
    msg_rcv=connexion.recv(1024) # msg = "OK nb_jobs"

    nb_jobs = int( msg_rcv.split()[1] )
    for i in range(nb_jobs):
        msg_rcv=connexion.recv(1024)  # Limit up 1 Mo 
	#print msg_rcv
	(job,status,node,date,time,long) = msg_rcv.split()
	if long:
	    longjob='L'
	else:
	    longjob=' '
	if status=='R':
	    print "%-34s %s  %s  %s %s %s"%(job,status,longjob,node,date,time)
	elif status=='Q':
	    print "%-34s %s  %s"%(job,status,longjob)
    connexion.close()

# Delete JOB 
elif sys.argv[1]=='DELJOB':

    if len(sys.argv)<3:
        print "Argument missing."
	sys.exit()

    connexion = connect()
    if connexion=='NULL':
        print 'No server found'
        sys.exit()

    msg_send="DELJOB %s"%(sys.argv[2])
    print msg_send
    connexion.send(msg_send)

    msg_rcv=connexion.recv(1024) # msg = "OK id_job" or "FAILED id_job"
    print msg_rcv

    connexion.close()

# Start cluster
elif sys.argv[1]=='START':

    if len(sys.argv)<3:
        print "No nodes given !"
        print "Will start master only"

    # Check if a server is already running
    connexion = connect()

    if connexion == 'NULL':
        cmd="/usr/sbin/daemonize -o /tmp/server.log -e /tmp/server.err /usr/local/bin/server.py"
        os.system(cmd)
    else:    
        msg_send="STATUS"
        connexion.send(msg_send)
        msg_rcv=connexion.recv(1024) # msg = "OK"
        if msg_rcv=='OK':
            print 'Server is already on !'
            connexion.close()
    
    # Will launch nodes in nodefile
    if len(sys.argv)>=3:
        try:
            nodefile = open(sys.argv[2])
	    for line in nodefile.readlines():
                # TODO : check if a slave is not already running on this node!
	        node=line.rstrip('\n')
	        # Check for comments
	        if node[0]!='#':
	            # Make sure that daemonize is installed on the node
	            print 'Starting slave.py on %s'%line
		    cmd="ssh %s \"/usr/local/bin/daemonize -o /tmp/slave.log -e /tmp/slave.err /usr/local/bin/slave.py\""%node
	            os.system(cmd)
		    time.sleep(0.1)
	    nodefile.close()
        except:
            print 'Cannot open Nodes list %s'%sys.argv[2]
            sys.exit()


elif sys.argv[1]=='HALT':
    connexion = connect()
    if connexion=='NULL':
        print 'No server found'
        sys.exit()

    msg_send="HALT"
    connexion.send(msg_send)
    print "Stopping the Cluster..."
    msg_rcv=connexion.recv(1024) # msg = "OK nb_nodes"
    connexion.close()

elif sys.argv[1]=='STARTNODE':
    connexion = connect()
    if connexion=='NULL':
        print 'No server found'
        sys.exit()

    print 'Starting slave.py on %s'%(sys.argv[2])
    cmd="ssh %s \"/usr/local/bin/daemonize -o /tmp/slave.log -e /tmp/slave.err /usr/local/bin/slave.py\""%(sys.argv[2])
    os.system(cmd)

elif sys.argv[1]=='HALTNODE':
    connexion = connect()
    if connexion=='NULL':
        print 'No server found'
        sys.exit()

    msg_send="HALTNODE %s"%(sys.argv[2])
    connexion.send(msg_send)
    msg_rcv=connexion.recv(1024) # msg = "OK"
    if msg_rcv=='OK':
        print 'Node %s stopped'%sys.argv[2]
    connexion.close()


elif sys.argv[1]=='RESERVE':
    connexion = connect()
    if connexion=='NULL':
        print 'No server found'
        sys.exit()

    msg_send="RESERVE %s"%(sys.argv[2])
    connexion.send(msg_send)
    msg_rcv=connexion.recv(1024) # msg = "OK"
    if msg_rcv=='OK':
        print 'Reserve %s'%sys.argv[2]
    elif msg_rcv=='ERROR':
        print 'Error : %s'%sys.argv[2]
    connexion.close()

# Not a valid command
else : 
    print "Command %s not recognized"%sys.argv[1]


sys.exit()
            
