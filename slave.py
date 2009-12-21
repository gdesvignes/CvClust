#! /usr/bin/env python

#
#  Need to have daemonize installed on the nodes !!
#
#

import socket, sys, time, threading, os

os.system('export PGPLOT_DIR="/usr/local/pgplot"')
os.system('export PATH=$PATH:/usr/local/presto/bin:/usr/local/bin')
os.environ['TEMPO'] = "/usr/local/tempo"
os.environ['PYTHONPATH'] = "/usr/local/presto/lib/python"

from NBPP_presto_search import *
from BON_128_presto_search import *

host = '192.168.1.50' # Changed from XX.XX.XX.250, 20 avril 2009
port = 46000
N_CPU = 4

# Base directory
basedir = '/root'


def close():
    halt = 1

class Thread(threading.Thread):
    """objet thread"""
    def __init__(self, conn, msg_rcv):
        threading.Thread.__init__(self)
        self.connection = conn           # ref. du socket de connexion
	self.msg_rcv=msg_rcv
        
    def run(self):

        command=self.msg_rcv.split()[0]
	#print "command received %s"%command

	if command=='JOB':
	    job_id,core_name,box = self.msg_rcv.split()[1:4]
	    
	    print job_id,core_name,box
	    
	    logfile.write("%s> Received Job %s on %s\n"%(time.asctime(),job_id,core_name))
	    logfile.flush()

	    # Send START signal
	    msg_send = "JOB %s %s START"%(job_id,core_name)  
	    self.connection.send(msg_send)
	    print "Sent : %s"%msg_send
	    
	    # Post the script-worker you want here
	    pid = os.fork()
	    if pid == 0:      # Process fils

	        # nb_cands = -1 if observation is too short 
	        # nb_cands = -2 if observation is too contaminated 
		if job_id.endswith(".fb"):
	            nb_cands = main_process(job_id,int(core_name.split("-")[1]),box)
		elif job_id.endswith(".fbk"):
	            nb_cands = main_process_bon(job_id,int(core_name.split("-")[1]),box)
		
	        # Send FINISHED signal
	        msg_send = "JOB %s %s FINISHED %d"%(job_id,core_name,nb_cands)
                self.connection.send(msg_send)
		os._exit(0)
		
		
	    else:             # Process pere en attente du fils
	        (rpid, status) = os.wait()
	        
	    # Send FINISHED signal
	    #msg_send = "JOB %s %s FINISHED %d"%(job_id,core_name,nb_cands)
            #self.connection.send(msg_send)

        if command=='PING':
	    msg_send = "PING %s"%node_name
	    self.connection.send(msg_send)
	
	if command=='CLOSE':
	    close()
	    
	if command=='HALTNODE':
  	    print "HALTNODE"
	    #self.connection.close()
	    sys.exit()
	    
	#ADD a shutdown

	#ADD a kill

        thread._Thread__stop()

# Global vars
halt = 0

# Open logfile in append mode
logfile = open("%s/log.dat"%basedir,'a')

# Waiting for connection to the server
connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
while 1:
    try:
        connection.connect((host, port))
	break
    except socket.error:
        #print "Connexion failed. Waiting..."
        time.sleep(5)    
logfile.write("%s> Connected to the server.\n"%time.asctime())
logfile.flush()

node_name = socket.gethostname()

# First Say connect to the server
msg_send="CONNECT %s %d"%(node_name,N_CPU)
connection.send(msg_send)
            
# Main loop	    
while 1:

    if halt == 1:
        break
    msg_rcv = connection.recv(1024)
    thread = Thread(connection, msg_rcv)
    thread.start()

