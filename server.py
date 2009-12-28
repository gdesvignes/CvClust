#! /usr/bin/env python


#

# IP adress of the master
HOST = '192.168.1.50' # Changed 20 avril 2009
HOST2 = '127.0.0.1'
PORT = 46000
PORT2 = 47000

NB_CORE_MAX = 4

# Base directory
basedir = '/root'

# Results directory
resultsdir = '/data6/results/'

import socket, sys, time, threading, os, sigproc

def get_time():
    stime='%02d:%02d:%02d %02d/%02d/%04d'%(time.localtime()[3],time.localtime()[4],time.localtime()[5],time.localtime()[2],time.localtime()[1],time.localtime()[0])
    return stime

def write_jobs_queue():
    file = open("%s/queue.dat"%basedir, "w")
    for job in jobs_list:
        file.write("%s %s\n"%(job,jobs[job].box))
    file.close()

def read_jobs_queue(jobs):
    nb_jobs_sub=0
    try:
        file = open("%s/queue.dat"%basedir, "r")
        for line in file.readlines():

	    #
	    (job,box) = line.split()
	    print job,box
            box=box.replace("\n","")

            jobs_list.append(job)
            jobs_wlist.append(job)
	    jobs[job]=Job()
	    jobs[job].box=box

	    # Determine size 
	    fil_filenm = "/data5/%s"%job.replace(".fbk","_p00.fbk")
	    filhdr, hdrlen = sigproc.read_header(fil_filenm)
	    orig_N = sigproc.samples_per_file(fil_filenm, filhdr, hdrlen)

	    if orig_N > 4194304:
	      logfile.write('Job %s detected L'%job);logfile.flush()
	      jobs[job].long=1  
	    ###################  

	    nb_jobs_sub += 1
        file.close()
        return nb_jobs_sub 	
    except:
        return 0


def submit_jobs(filename):
  """
  To Submit a Job : 

  jobs_list(job_id)
  jobs_wlist(job_id)
  jobs[job_id]=Job()
  jobs[job_id].box_id
  """

  nb_jobs=0
  try:
    print "Try to open %s"%filename
    file = open(filename, "r")
    for job in file.readlines():

        job=job.replace("\n","")

        jobs_list.append(job)
        jobs_wlist.append(job)

	jobs[job]=Job()

	# Determine size 
	fil_filenm = "/data5/%s"%job.replace(".fbk","_p00.fbk")
	filhdr, hdrlen = sigproc.read_header(fil_filenm)
	orig_N = sigproc.samples_per_file(fil_filenm, filhdr, hdrlen)

	if orig_N > 4194304:
	  logfile.write('Job %s detected L\n'%job);logfile.flush()
	  jobs[job].long=1  
	###################  

        c=filename.index("/")
	jobs[job].box=filename[c+1:]

	nb_jobs += 1
    file.close()

    write_jobs_queue()	
    return nb_jobs 	
  except: return 0

def submit_a_job(job,box):
    """
    To resubmit a job after deletion by 'remove_a_job'
    """

    jobs_list.append(job)
    jobs_wlist.append(job)
    jobs[job]=Job()
    jobs[job].box=box

    # Determine size 
    fil_filenm = "/data5/%s"%job.replace(".fbk","_p00.fbk")
    filhdr, hdrlen = sigproc.read_header(fil_filenm)
    orig_N = sigproc.samples_per_file(fil_filenm, filhdr, hdrlen)

    if orig_N > 4194304:
      logfile.write('Job %s detected L'%job);logfile.flush()
      jobs[job].long=1  
    else:
      jobs[job].long=0
    ###################  

    write_jobs_queue()

def remove_a_job(job):
    """
    Remove a job from the database. ie: A node has crashed
    Should resubmit it after with 'submit_a_job'
    """
    try:
      jobs_list.remove(job)
      jobs_wlist.remove(job)
      del jobs[job]
      return 1
    except:
      return 0


def check_nodes(nodes):
    """
    Check the nodes which are IDLE or BUSY
    Send the PING command
    """

    #print nodes
    msg_send='PING'

    for node in nodes.keys():
      #if 'IDLE' in nodes[node].status[0] or 'BUSY' in nodes[node].status[0]:  
        #print "Try to send %s to %s"%(msg_send,node)
        try:
            nodes[node].conn.send(msg_send)
        except socket.error, e:
            for i in range(nodes[node].nb_cpus):
                nodes[node].status[i]='OFFLINE'

		# Check if jobs and restart them 
		job=nodes[node].job[i]
		#box=jobs[job].box
		if not "none" in job: 
		    nodes[node].job[i]='none'  # Remove job in the node status
		    box=jobs[job].box
		    remove_a_job(job)
		    submit_a_job(job,box)
	    try:    
                nodes[node].conn.close()
	    except:
	        pass

        except IOError, e:
            for i in range(nodes[node].nb_cpus):
                nodes[node].status[i]='OFFLINE'

		# Check if jobs and restart them 
		job=nodes[node].job[i]
		#box=jobs[job].box
		if not "none" in job:
		    nodes[node].job[i]='none'  # Remove job in the node status
		    box=jobs[job].box
		    remove_a_job(job)
		    submit_a_job(job,box)
	    try:    
                nodes[node].conn.close()
	    except:
	        pass
	        
class Job:
    '''Job info for database. Key is the jobfilename'''
    def __init__(self):
        self.status="Q"
        self.node="none"
        self.t_start="none none" 
        self.t_end="none none"
	self.nb_cands=0
	self.box="none"
	self.long=0  # USE THREADS

class Node:
    '''Node Status'''
    def __init__(self,node_name,nb_cpus,conn):
        self.node_name=node_name
	self.nb_cpus=nb_cpus
	self.conn=conn
        self.status=[]
        self.info=[]
        self.core_name=[]
	self.job=[]
	self.reserved=0


class ThreadCommand(threading.Thread):
    '''thread pour gerer la reception des commandes'''
    def __init__(self):
        threading.Thread.__init__(self)
        
    def run(self):

        # Initialisation du serveur - Mise en place du socket pour recevoir les ordres:
        sock_com = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock_com.bind((HOST2, PORT2))
        except socket.error:
            print "ThreadCommand failed !",socket.error
            sys.exit()
        sock_com.listen(2)
        logfile.write("%s> Server ready to receive commands ...\n"%time.asctime())

        # Dialogue avec le client :
        nom = self.getName()            # Chaque thread possede un nom
        while 1:

	    # Receive command
	    connection, adresse = sock_com.accept()
            msg_rcv = connection.recv(1024)
	    command=msg_rcv.split()[0]

            # Submit new jobs
	    if command=='SUBMIT':       
	        filename=msg_rcv.split()[1]
		nb_jobs=submit_jobs(filename)
		msg_send="OK %d"%(nb_jobs)
		logfile.write("%d jobs submitted"%nb_jobs);logfile.flush()
		connection.send(msg_send)

	    # Nodes infos	
 	    elif command=='INFO':
	        msg_send="OK %d"%(len( nodes.keys() ))
		connection.send(msg_send)
		time.sleep(0.1) 
		for node in nodes.keys():
		    msg_send="%s %d %s %d"%(nodes[node].node_name,nodes[node].nb_cpus," ".join(nodes[node].status),nodes[node].reserved)
		    print msg_send
		    connection.send(msg_send)
		    time.sleep(0.001)

	    # List status of job	    
	    elif command=='LISTJOB':
	        msg_send="OK %d"%(len(jobs))
		connection.send(msg_send)
		time.sleep(0.1) 
		for job in jobs_list:
		    msg_send="%s %s %s %s %d"%(job,jobs[job].status,jobs[job].node,jobs[job].t_start,jobs[job].long)
		    print msg_send
		    connection.send(msg_send)
		    time.sleep(0.001)

	    # List status of job	    
	    elif command=='DELJOB':
	        job2del=msg_rcv.split()[1]
		status=remove_a_job(job2del)

		if(status):
		    msg_send="OK %s"%(job2del)
		else:
		    msg_send="FAILED %s"%(job2del)
		logfile.write("DELJOB %s"%msg_send);logfile.flush()
		connection.send(msg_send)
	        

	    # Stop cluster	
	    elif command=='HALT':
	        print "Call to HALT"
	        msg_send="OK"
		connection.send(msg_send)
		for node in nodes.keys():
		    cmd="ssh %s \"killall python\""%node
		    print cmd
		    os.system(cmd)
		cmd="killall python"    
		os.system(cmd)
		sys.exit(0);    


	    # Stop a node	
	    elif command=='HALTNODE':
	        selected_node = msg_rcv.split()[1]
		print "Call to stop node %s"%selected_node
		if nodes.has_key(selected_node):
		    cmd="ssh %s \"killall python\""%selected_node
		    print cmd
		    os.system(cmd)
		msg_send="OK"
		connection.send(msg_send)

	    # Reserve a node	
	    elif command=='RESERVE':
	        selected_node = msg_rcv.split()[1]
		if nodes.has_key(selected_node):
		    print "Want to reserve node %s for threaded jobs"%selected_node
		    if nodes[selected_node].reserved==1:
		      nodes[selected_node].reserved=0
		    elif nodes[selected_node].reserved==0:
		      nodes[selected_node].reserved=1
		    msg_send="OK"
		else:
		    msg_send="ERROR"
		connection.send(msg_send)

	    # Ask for status - Send OK	
	    elif command=='STATUS':
	        msg_send="OK"
		connection.send(msg_send)

            connection.close()      # couper la connexion cote serveur
	    #time.sleep(1)

	print "Exit thread for command.py"
	        

class ThreadJob(threading.Thread):
    '''thread pour lancer les jobs'''
    def __init__(self):
        threading.Thread.__init__(self)
        
    def run(self):

        # Loop to submit jobs every 5s
	while 1:

	    submitted=0


	    # Check nodes status 
	    check_nodes(nodes)


	    # Main Loop : if there is jobs waiting	
            if len(jobs_wlist) > 0:
		logfile.write("long job ? %d\n"%jobs[jobs_wlist[0]].long);logfile.flush()


	        # Job long - Cherche un noeud reserve a 4 coeurs libre pour threader le process
	        if jobs[jobs_wlist[0]].long:
		  for node in nodes.keys():
	            if nodes[node].nb_cpus == 4 and nodes[node].reserved==1 and nodes[node].status[0]=='IDLE' and nodes[node].status[1]=='IDLE' and nodes[node].status[2]=='IDLE' and nodes[node].status[3]=='IDLE':
		      msg_send="JOB_L %s %s %s"%(jobs_wlist[0],nodes[node].core_name[0],jobs[jobs_wlist[0]].box)
		      logfile.write(msg_send);logfile.flush()
		      nodes[node].conn.send(msg_send)

		      directory = "%s/%s"%(resultsdir,jobs[jobs_wlist[0]].box)

		      try:
		          os.makedirs(directory)
		      except:
	   	          pass
			   
    	              jobs[jobs_wlist[0]].status = 'R'  # Job status is running
		      del jobs_wlist[0]                 # Remove Job from the waiting queue

		      submitted=1
		      break
	          #if submitted==1: break

		else:  # Job court
                  # Loop over all CPU0, then CPU1, CPU2, assuming a max of NB_CORE_MAX cores per computer 
		  logfile.write('Job court\n');logfile.flush()
		  for i in range(NB_CORE_MAX):

		    #print node.node_name
		    #for i in range(nodes[node].nb_cpus):
		    for node in nodes.keys():

		      # Noeud non reserve
                      if nodes[node].reserved==0:
		       if i<nodes[node].nb_cpus:   # Si moins de NB_CORE_MAX cores
	                if nodes[node].status[i]=='IDLE':
                            #print "coeur dispo : %s"%(nodes[node].core_name[i])
                            #print "Submit JOB %s on %s"%(jobs_wlist[0],nodes[node].core_name[i])
			    msg_send="JOB %s %s %s"%(jobs_wlist[0],nodes[node].core_name[i],jobs[jobs_wlist[0]].box)
		            logfile.write(msg_send);logfile.flush()
			    nodes[node].conn.send(msg_send)
			    directory = "%s/%s"%(resultsdir,jobs[jobs_wlist[0]].box)

			    try:
			        os.makedirs(directory)
		            except:
			        pass
			   
		            jobs[jobs_wlist[0]].status = 'R'  # Job status is running
		            del jobs_wlist[0]                 # Remove Job from the waiting queue

			    submitted=1
			    break
	            if submitted==1: break

            # Sleep for 5s before to loop
            time.sleep(5)
	    write_jobs_queue()
       
     
class ThreadNode(threading.Thread):
    '''thread pour gerer la reception des commandes'''
    def __init__(self, conn):
        threading.Thread.__init__(self)
	self.connection = conn 
#	self.msg_rcv=msg_rcv
        
    def run(self):
      while 1:
        try:
            msg_rcv = self.connection.recv(1024)
	except socket.error, e:
	    #print "ThreadNode error1"
            self.connection.close() #MODI
	    break

	except IOError, e:
	    #print "ThreadNode error2"
            self.connection.close() #MODI 
	    break

	if len(msg_rcv)>1:
          command=msg_rcv.split()[0]

          if command=='CONNECT':        # A node want to connect
            node_name=msg_rcv.split()[1]
	    nb_cpus=int(msg_rcv.split()[2])

	    # If known node
	    if nodes.has_key(node_name):
	        logfile.write("%s> %s Reconnected\n"%(time.asctime(),node_name))
		nodes[node_name].conn=self.connection
	        for i in range(nodes[node_name].nb_cpus):
	            nodes[node_name].status[i]='IDLE'
	            nodes[node_name].job[i]='none'
	            #nodes[node_name].core_name[i]=('%s-%d'%(node_name,i))

            # New node
	    else:
                nodes[node_name]=Node(node_name,nb_cpus,self.connection)
	        #nodes_name.append(node_name)
	        for i in range(nodes[node_name].nb_cpus):
	            nodes[node_name].status.append('IDLE')
	            nodes[node_name].core_name.append('%s-%d'%(node_name,i))
	            nodes[node_name].job.append('none')


	        logfile.write("%s> %s added to nodelist\n"%(time.asctime(),node_name))
	        print "%s> %s added to nodelist\n"%(time.asctime(),node_name)
	        logfile.flush()

          if command=='JOB':            # Report from a node about a JOB
            job = msg_rcv.split()[1] 
	    core_name = msg_rcv.split()[2]
	    (node_name,core_idx) = core_name.split('-')
            i=int(core_idx)

	    print "%s %s %s %d\n"%(msg_rcv,job,node_name,i)

	    if msg_rcv.split()[3]=='START':
	        #print 'DEBUG : received START from node, msg=%s'%msg_rcv
	        nodes[node_name].status[i]='BUSY'
		nodes[node_name].job[i]=job
		#jobs[job].status = 'R'

		#print 'DEBUG : jobs:',jobs

		jobs[job].node = node_name
		jobs[job].t_start = get_time()

	    elif msg_rcv.split()[3]=='JOIN-RFI':
	        nodes[node_name].status[i]='JOIN-RFI'
            
	    elif msg_rcv.split()[3]=='PREP-ACCEL-SP':
	        nodes[node_name].status[i]='PREP-ACCEL-SP'
            
	    elif msg_rcv.split()[3]=='SIFT':
	        nodes[node_name].status[i]='SIFT'
            
	    elif msg_rcv.split()[3]=='FOLDING':
	        nodes[node_name].status[i]='FOLDING'
            
	    elif msg_rcv.split()[3]=='COMPRESS':
	        nodes[node_name].status[i]='COMPRESS'
            
	    elif msg_rcv.split()[3]=='FINISHED':
	        nodes[node_name].status[i]='IDLE'
		nodes[node_name].job[i]='none'
		jobs[job].t_end = get_time()
		jobs[job].nb_cands = msg_rcv.split()[4]

		# Add infos to database
		dbfile.write("%s %s %s %s %s\n"%(job,jobs[job].node,jobs[job].t_start,jobs[job].t_end,jobs[job].nb_cands))
	        dbfile.flush()

		# Remove job
		del( jobs_list[jobs_list.index(job)] )
		del(jobs[job])

        #core_name=msg_rcv.split()[1] 
      print "exit thread"



###########################################################################



jobs = {}                # dictionnaire des jobs
jobs_list = []           # liste des jobs en attente et en cours de traitement
jobs_wlist = []          # liste des jobs en attente
box_list = []

cores = {}		 
nodes = {}               # Dictionnaire des noeuds

# Open logfile in append mode
logfile = open("%s/log.dat"%basedir,'a')

# Open database in append mode
dbfile = open("%s/database.dat"%basedir,'a')

# Threads to receive orders
thread_com = ThreadCommand()
thread_com.start()

# Thread for jobs
thread_job = ThreadJob()
thread_job.start()

# Initialisation du serveur :
con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    con.bind((HOST, PORT))
except socket.error:
    logfile.write("%s> Initialisation of server failed !\n"%time.asctime())
    sys.exit()
logfile.write("%s> Server ready, waiting for connections...\n"%time.asctime())
logfile.flush()
con.listen(64)

# First read the queue.log
logfile.write("%s> Read %d jobs from queue.log\n"%(time.asctime(),read_jobs_queue(jobs)))

# Main loop
while 1:

    connection, adresse = con.accept()  
    
    thread_node = ThreadNode(connection)
    thread_node.start()


sys.exit()

