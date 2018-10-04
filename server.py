from socket import *
import os
import random
from threading import Thread
import sys
import numpy as np
import subprocess
import ast
import marshal
import datetime
import logging
import copy

#logging setup
logging.basicConfig(filename='log', level=logging.INFO, filemode='w')

#LOG = sys.argv[1]
SERVERNAME = sys.argv[1]
myHost = ''
myPort = 2000
MACHINE = 'fa17-cs425-g25-{server_id}.cs.illinois.edu'
serverPort = 2000
JOINPORT = 2001
NEWJOINPORT = 2002
s = socket(AF_INET, SOCK_STREAM)    # create a TCP socket
s.bind((myHost, myPort))            # bind it to the server port
s.listen(10)                         # allow 10 simultaneous
                                    # pending connections
INTRODUCER = '01'
memberlist = {}
JOINMSG = 'join-{server_name}'

#This method sends a message to the introducer in order to join
def join():
	global memberlist
	join_message = JOINMSG.format(server_name=SERVERNAME) 
	intro = MACHINE.format(server_id=INTRODUCER) 		
	sock = socket(AF_INET, SOCK_DGRAM)
	sock.settimeout(2)
        server_addr = (intro, JOINPORT)
	join_message = marshal.dumps(join_message)
	sock.sendto(join_message, server_addr)
	try:
		response, address = sock.recvfrom(4096)
		if response:	#The introducer is up and join is successful
			memberlist = marshal.loads(response)
			time = str(datetime.datetime.utcnow())
			if INTRODUCER not in memberlist:
				memberlist[INTRODUCER] = ([time], True)	
			print 'joined:', memberlist
		sock.close()
		return
	except timeout:
		print 'join timeout'
	except error as e:
		print 'join error', e
	finally:
		sock.close()
		return
	


#Method to receive all grep command from client and call it on the machine's log file
#Sever will execute the command and first send back the size of the resulting answer so the client will know when to stop reading
#When client sends back acknowledgment that it has recevied the lenght of results, the server responds with the actual results
#Set a timeout to prevent hanging when listening for data and none comes 
#def parse_log(data, connection):
#	    try: 
#	    	cmd = data + ' ' + LOG
#	    	cmd2 = data + ' ' + LOG + ' | wc -l'
#	    	output = subprocess.check_output(cmd, shell=True)
#	    	output2 = subprocess.check_output(cmd2, shell=True)
#	    except:
#		output = ''
#		output2 = 0
#	    try:
 #           	connection.sendall(str(len(output)))
  #          except error:
#		'print file_szie err'
#	    while 1:
 #               try:
  #
#                     file_size = connection.recv(1024)
#		       if file_size:
#				connection.sendall(str(output) + '\n line count ' + SERVERNAME + ': ' + str(output2))
#		       else:
#				break
#		except error:
#			print 'log err'
#			break


		

#Waits to receive data from connection
#method to determine what kind of data is being sent by client: a ping, ping req (# of machine to ping), or grep command
#for each type of data, a separate method is called to handle it
#def analyze_data(connection):
#	while 1:
#		try:
 #       		data = connection.recv(1024) # receive up to 1K bytes
			#if data == 'ping':
				#p_thread = Thread(target=ping, args=(connection,))
        			#p_thread.daemon = True
        			#p_thread.start()
				#ping(connection)
        		#elif 'ping_req' in data:
				#udp_thread = Thread(target=udp_server)
        			#udp_thread.daemon = True
        			#udp_thread.start()
				#ping_req(data, connection)
 #       		if data:
#				parse_log(data, connection)
#			else:
#				break
#		except error:
#			break

#def sort_members(a, b):
#	if a>b:
#		return 1
#	return -1

#def send_leave_message(sorted_available, suc):	
#	sock = socket(AF_INET, SOCK_DGRAM)
 #       server_addr = (MACHINE.format(server_id = sorted_available[suc]), serverPort)
#	leave_message = 'left-{server_id}'
#	leave_message = leave_message.format(server_id = SERVERNAME)
#	leave_message = marshal.dumps(leave_message)
#	sock.sendto(leave_message)
#	sock.close()
	
	
#def leave():
#	sorted_available = sorted(available, cmp=sort_members)
#	myindex = sorted_available.index(SERVERNAME)
#	if myindex!=0:
#		pre = myindex - 1
#	else:
#		pre = 0
#	suc = myindex + 1	
#	send_leave_message(sorted_available, suc)
#	send_leave_message(sorted_available, pre)
		
	
def get_command():
	while 1:
		print 'Enter "j" to join, "l" to leave, "m" to list the membership list, and "s" to list self id'
		line = sys.stdin.readline().strip('\n')
		if line == 'j':
			join()
		elif line == 'm':
			print memberlist
		elif line == 's':
			print SERVERNAME
		elif line == 'l':
			leave()			

#Waits for incoming connections to tcp socket
def main():
	while 1:
    		connection, address = s.accept() 
   		connection.settimeout(10)
		analyze_data(connection)
		connection.close()

#Creates a thread to handle incoming connections from udp socket for disseminate requests from client while main thread is running
if __name__ == "__main__":
	command_thread = Thread(target = get_command)
	command_thread.daemon = True
	command_thread.start()
	main() 

