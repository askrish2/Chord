from socket import *
import os
import random
from threading import Thread
from threading import Lock
import sys
import numpy as np
import subprocess
import ast
import marshal
import datetime
import logging
import copy
import time

#logging setup
logging.basicConfig(filename='log', level=logging.INFO, filemode='w')

#LOG = sys.argv[1]
STR_DATETIME = '%Y-%m-%d %H:%M:%S.%f'
SERVERNAME = sys.argv[1]
myHost = ''
myPort = 2000
MACHINE = 'fa17-cs425-g25-{server_id}.cs.illinois.edu'
serverPort = 2000
JOINPORT = 2001
HB_BASE_SENDING_SUC = 2005
HB_BASE_WAITING_PRE = 2005
HB_BASE_SENDING_PRE = 2017
HB_BASE_WAITING_SUC = 2017
LISTENJOINPORT = 2002
INACTIVEPORT = 2003
FAILUREPORT = 2004
s = socket(AF_INET, SOCK_STREAM)    # create a TCP socket
s.bind((myHost, myPort))            # bind it to the server port
s.listen(10)                         # allow 10 simultaneous
                                    # pending connections
INTRODUCER = '01'
members = []
pre = ''
suc = ''
memberlist = {}
JOINMSG = 'join-{server_name}'

members_mutex = Lock()
pre_mutex = Lock()
suc_mutex = Lock()
memberlist_mutex = Lock()

def send_hb_suc():
        while 1:
                if suc != '':
                        base = HB_BASE_SENDING_SUC
			suc_mutex.acquire()
                        port = base + int(suc)
                        machine = MACHINE.format(server_id=suc) 
			suc_mutex.release()
			sock = socket(AF_INET, SOCK_DGRAM)
                        server_addr = (machine, port)
                        hb_message = SERVERNAME
                        hb_message = marshal.dumps(hb_message)
                        sock.sendto(hb_message, server_addr)
        #               print 'sent', suc, 'port', port
                        sock.close()

def send_hb_pre():
        while 1:
                if pre != '':
                        base = HB_BASE_SENDING_PRE
			pre_mutex.acquire()
                        port = base + int(pre)
                        machine = MACHINE.format(server_id=pre)
			pre_mutex.release()
                        sock = socket(AF_INET, SOCK_DGRAM)
                        server_addr = (machine, port)
                        hb_message = SERVERNAME
                        hb_message = marshal.dumps(hb_message)
                        sock.sendto(hb_message, server_addr)
        #               print 'sent', pre, 'port', port
                        sock.close()
def wait_hb_suc():
        base = HB_BASE_WAITING_SUC
        port =  base + int(SERVERNAME)
        sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (myHost, port)
        sock.bind(server_addr)
        sock.settimeout(3)
        failed_once = False
        while 1:
                if suc!='' and len(members)>=3:
                        try:	
				failed_once = False
                                data, address = sock.recvfrom(4096)
                                data = marshal.loads(data)
                #               print 'wait hb', suc, 'port', port
                        except timeout:
                                if failed_once:
                                        print 'failed', suc
                                        failed_once = False
                                        mark_failed(suc)
                                        continue
                                else:
                                        failed_once = True
                                        print 'failed once', suc
                                        continue
def wait_hb_pre():
        base = HB_BASE_WAITING_PRE
        port =  base + int(SERVERNAME)
        sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (myHost, port)
        sock.bind(server_addr)  
        sock.settimeout(3)
        failed_once = False
        while 1:
                if pre!='' and len(members)>=3:
                        try:
                                data, address = sock.recvfrom(4096)
                                data = marshal.loads(data)
                               # print 'wait hb', pre, 'port', port
                        except timeout:
                                if failed_once:
                                        print 'failed', pre
                                        failed_once = False
                                        mark_failed(pre)
                                        continue
                                else:
                                        failed_once = True
                                        print 'failed once', pre
                                        continue
			failed_once = False
	
def listen_inactive():
	print 'what'
	port = INACTIVEPORT
	sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (myHost, port)
        sock.bind(server_addr)
	while 1:
		data, address = sock.recvfrom(4096)
		(m, time) = marshal.loads(data)
		time = datetime.datetime.strptime(time, STR_DATETIME)
		memberlist_mutex.acquire()
		(my_times, flag) = memberlist[m]
		memberlist_mutex.release()
		my_time = my_times[len(my_times)-1]	
		my_time = datetime.datetime.strptime(my_time, STR_DATETIME)
		latest = max((time, my_time))
                if latest == my_time or not flag:
			continue
		else:
			print 'marked inactive', m
			mark_inactive(m)		

def mark_inactive(m):
	global pre, suc
	(times, flag) = memberlist[m]
	memberlist_mutex.acquire()
	memberlist[m] = (times, False)
	memberlist_mutex.release()
	port = INACTIVEPORT
        machine = MACHINE.format(server_id=pre)
        sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (machine, port)
       	inactive_message = (m, str(datetime.datetime.utcnow()))
        inactive_message = marshal.dumps(inactive_message)
        sock.sendto(inactive_message, server_addr)	
	sock.close()
	machine = MACHINE.format(server_id=suc)
	sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (machine, port)
	sock.sendto(inactive_message, server_addr)	
	if m != SERVERNAME:
		update_pre_suc()
	else:
		pre_mutex.acquire()
		pre = ''
		pre_mutex.release()
		suc_mutex.acquire()
		suc = ''
		suc_mutex.release()

def update_pre_suc():
	global members, pre, suc
	members_mutex.acquire()
	memberlist_mutex.acquire()
	members = []
	for key, (times, active) in memberlist.iteritems():
		if active:
			members.append(key)
	len_mems = len(members)
	if len_mems >= 3:
		members = sorted(members)
		my_index = members.index(SERVERNAME)
		pre_ind = my_index -1
		suc_ind = my_index +1
		pre_mutex.acquire()
		suc_mutex.acquire()
		if pre_ind is -1:
			pre = members[len_mems-1]
		else:
			pre = members[pre_ind]
		if suc_ind is (len_mems):
			suc = members[0]
		else:
			suc = members[suc_ind]
		pre_mutex.release()
		suc_mutex.release()
		print 'pre:', pre, 'suc', suc
	memberlist_mutex.release()
	members_mutex.release()	

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
		update_pre_suc()
		return
	except timeout:
		print 'join timeout'
	except error as e:
		print 'join error', e
	finally:
		sock.close()
		return

def listen_joins():
	print 'listen' 
	global memberlist
	sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (myHost, LISTENJOINPORT)
        sock.bind(server_addr)	
	while 1:
		data, address = sock.recvfrom(4096)
		(member_id, member_data) = marshal.loads(data)
		if member_id in memberlist:
			my_times = memberlist[member_id][0]
			my_latest_time = my_times[len(my_times)-1]
			my_latest_time_dt = datetime.datetime.strptime(my_latest_time, STR_DATETIME)
			sent_times = member_data[0]
			sent_latest_time = sent_times[len(sent_times)-1]				
			sent_latest_time_dt = datetime.datetime.strptime(sent_latest_time, STR_DATETIME)
			latest = max((sent_latest_time, my_latest_time))
			if latest == my_latest_time_dt:
				continue
		memberlist[member_id] = member_data	
		update_pre_suc()
		print 'new join:', memberlist
	sock.close()
				
					
	
def listen_failed():
	print 'listen failed'
        port = FAILUREPORT
        sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (myHost, port)
        sock.bind(server_addr)
        while 1:
		print 'hi'
                data, address = sock.recvfrom(4096)
                (m, time) = marshal.loads(data)
		print 'found failed', m
                time = datetime.datetime.strptime(time, STR_DATETIME)
                memberlist_mutex.acquire()
		if m in memberlist:
			(my_times, flag) = memberlist[m]
			memberlist_mutex.release()
               		my_time = my_times[len(my_times)-1]
               		my_time = datetime.datetime.strptime(my_time, STR_DATETIME)
                	latest = max((time, my_time))
                	if latest == my_time:
                        	continue
               		else:
                        	print 'marked failed', m
                       		mark_failed(m)
		else:
			mark_failed(m)

def mark_failed(m):
        memberlist_mutex.acquire()
	if m in memberlist:	
        	(times, flag) = memberlist[m]
        	del memberlist[m]
        memberlist_mutex.release()
        port = FAILUREPORT
        machine = MACHINE.format(server_id=pre)
        sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (machine, port)
        failure_message = (m, str(datetime.datetime.utcnow()))
        failure_message = marshal.dumps(failure_message)
        sock.sendto(failure_message, server_addr)
        sock.close()
        machine = MACHINE.format(server_id=suc)
        sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (machine, port)
        sock.sendto(failure_message, server_addr)
        sock.close()
        update_pre_suc()
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
def leave():
	print 'need to implement leave()'
	return		
	
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
			mark_inactive(SERVERNAME)
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
	listen_joins_thread = Thread(target = listen_joins)
	listen_joins_thread.daemon = True
	listen_joins_thread.start()
	
	listen_failed_thread = Thread(target = listen_failed)
        listen_failed_thread.daemon = True
        listen_failed_thread.start()
	listen_inactive_thread = Thread(target = listen_inactive)
	listen_inactive_thread.daemon = True
	listen_inactive_thread.start()
	
	for i in range(0,3):
                send_hb_thread_pre = Thread(target = send_hb_pre)
                send_hb_thread_suc = Thread(target = send_hb_suc)
                send_hb_thread_pre.daemon = True
                send_hb_thread_suc.daemon = True
                send_hb_thread_pre.start()
                send_hb_thread_suc.start() 	
	wait_hb_thread_pre = Thread(target = wait_hb_pre)
        wait_hb_thread_suc = Thread(target = wait_hb_suc)
        wait_hb_thread_pre.daemon = True
        wait_hb_thread_suc.daemon = True
        wait_hb_thread_pre.start()
        wait_hb_thread_suc.start()		
	
	main() 

