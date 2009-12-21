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
from BON_128_presto_search_thd import *

host = '192.168.1.50' # Changed from XX.XX.XX.250, 20 avril 2009
port = 46000
N_CPU = 4

# Base directory
basedir = '/root'


class Thread_accel(threading.Thread):
    """objet thread pour la dedispersion, recherche en acceleration, singlepulse"""
    def __init__(self, plan_id, job, ddplan):
        threading.Thread.__init__(self)
	self.plan_id = plan_id
#	self.cpu_id = cpu_id
	self.job = job
	self.ddplan = ddplan

    def run(self):

        thd_process_bon(self.plan_id, self.job, self.ddplan)

class Thread_fold(threading.Thread):
    """objet thread pour le folding"""
    def __init__(self, cand_id, cand, job, ddplans):
        threading.Thread.__init__(self)
	self.cand_id = cand_id
	self.cand = cand
#	self.cpu_id = cpu_id
	self.job = job
	self.ddplans = ddplans

    def run(self):

        fold_process_bon(self.cand, self.job, self.ddplans)

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

	# Threaded job
	if command=='JOB_L':
	    job_id,core_name,box = self.msg_rcv.split()[1:4]
	    print job_id,core_name,box
	    
	    logfile.write("%s> Received Job %s on %s\n"%(time.asctime(),job_id,core_name))
	    logfile.flush()

	    # Send START signal
	    msg_send = "JOB %s %s START"%(job_id,core_name)  
	    self.connection.send(msg_send)
	    #print "Sent : %s"%msg_send

	    # JOIN, RFIFIND
	    time.sleep(1)
	    task = "JOIN-RFI"
	    msg_send = "JOB %s %s %s"%(job_id,core_name,task)  
	    self.connection.send(msg_send)
	        # nb_cands = -1 if observation is too short 
	        # nb_cands = -2 if observation is too contaminated 
	    if job_id.endswith(".fbk"):
	        status,job,ddplans = preprocess_bon(job_id,box)
	        

	    # PREPSUBBAND, ACCEL, SINGLEPULSE
	    task = "PREP-ACCEL-SP"
	    msg_send = "JOB %s %s %s"%(job_id,core_name,task)  
	    self.connection.send(msg_send)
	    thread_process=[]
	    plan_id=0
 	    while plan_id < len(ddplans):
	    #for plan_id,ddplan in enumerate(ddplans):
	        if threading.activeCount() < 6:
		    thread_process.append(Thread_accel(plan_id,job,ddplans[plan_id]))
		    thread_process[plan_id].start()
		    print "Launch thread %d...  Nb actives threads=%d"%(plan_id,threading.activeCount())
		    plan_id+=1
		time.sleep(5)    
	    for plan_id,ddplan in enumerate(ddplans):
	        thread_process[plan_id].join()
		    
		
	    # SIFT
	    task = "SIFT"
	    msg_send = "JOB %s %s %s"%(job_id,core_name,task)  
	    self.connection.send(msg_send)
	    sift_process_bon(job)
		
	    # FOLDING
	    task = "FOLDING"
	    msg_send = "JOB %s %s %s"%(job_id,core_name,task)  
	    self.connection.send(msg_send)
	    thread_fold=[]
	    cands_folded = 0
 	    while cands_folded < len(job.all_accel_cands):
	        if cands_folded == max_cands_to_fold:
		    break
	        if threading.activeCount() < 6:
		    thread_fold.append(Thread_fold(cands_folded, job.all_accel_cands[cands_folded], job, ddplans))
		    thread_fold[cands_folded].start()
		    print "Launch thread...  Cands folded=%d"%(cands_folded)
		    cands_folded += 1
		time.sleep(5) # Launch one thread at a time, assure no problem with prepfold   
	    # Wait for all threads to finish	    
	    for i in range(cands_folded):
	        try:
		    thread_fold[i].join()
		except:
		    pass



            # Send START signal
            task = "COMPRESS"
            msg_send = "JOB %s %s %s"%(job_id,core_name,task)  
            self.connection.send(msg_send)

            compress_process_bon(job,box)
		
            # Send FINISHED signal
            msg_send = "JOB %s %s FINISHED %d"%(job_id,core_name,cands_folded)
            self.connection.send(msg_send)

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

    msg_rcv = connection.recv(1024)
    thread = Thread(connection, msg_rcv)
    thread.start()

