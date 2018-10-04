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
from multiprocessing.dummy import Pool as ThreadPool
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
HB_BASE = 2005
LISTENJOINPORT = 2002
INACTIVEPORT = 2003
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

def send_hb(m):
	while 1:
		if len(members) >= 3:
			if m:
				m = pre
			else:
				m = suc
			pre_mutex.acquire()
			port = HB_BASE + int(m)
			pre_mutex.release()
			machine = MACHINE.format(server_id=m)
			sock = socket(AF_INET, SOCK_DGRAM)
			server_addr = (machine, port)
			hb_message = SERVERNAME 
			hb_message = marshal.dumps(hb_message)
			sock.sendto(hb_message, server_addr)
			sock.close()

def wait_hb():
	port = HB_BASE + int(SERVERNAME)
	sock = socket(AF_INET, SOCK_DGRAM)
	server_addr = (myHost, port)
       	sock.bind(server_addr)
       	sock.settimeout(5)

	last_recv_pre = datetime.datetime.utcnow()
	last_recv_suc = datetime.datetime.utcnow()
	failed_once_pre = False
	failed_once_suc = False
	while 1:
		if len(members) >= 3:
			try:
				data, address = sock.recvfrom(4096)
				data = marshal.loads(data)
				print 'hb from', data
				if data == pre:
					last_recv_pre = datetime.datetime.utcnow()
				else:
					last_recv_suc = datetime.datetime.utcnow()
			except timeout:
				if last_recv_pre > last_recv_suc:
					if failed_once_pre:
						failed_once_pre = False
						mark_inactive(pre)
						continue
					else:
						failed_once_pre = True
						time.sleep(2)
						continue
				else:	
					if failed_once_suc:
						failed_once_suc = False
						mark_inactive(suc)
						continue
					else:
						failed_once_suc = True
						time.sleep(2)
						continue
					
def listen_inactive():
	port = INACTIVEPORT
	sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (myHost, port)
        sock.bind(server_addr)
	while 1:
		data, address = sock.recvfrom(4096)
		(m, time) = marshal.loads(data)
		time = datetime.datetime.strptime(time, STR_DATETIME)
		(my_times, flag) = memberlist[m]
		my_time = my_times[len(my_times)-1]
		print 'my times: ', my_times	
		my_time = datetime.datetime.strptime(my_time, STR_DATETIME)
		latest = max((time, my_time))
                if latest == my_time or not flag:
			continue
		else:
			print 'marked inactive', m
			mark_inactive(m)		

def mark_inactive(m):
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
	sock.close()
	update_pre_suc()	

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
			leave()			

def introducer():
        sock = socket(AF_INET, SOCK_DGRAM)
        server_addr = (myHost, JOINPORT)
        sock.bind(server_addr)
        while 1:
                data, address = sock.recvfrom(4096)
                data = marshal.loads(data)
                if 'join' in data:
                        num = data.split('-')[1]
                        if num in memberlist:
                                time = str(datetime.datetime.utcnow())
                                (times, active) = memberlist[num]
                                print 'times', times
                                times.append(time)
                                print times
                                memberlist[num] = (times,True)
                        else:
                                time =  str(datetime.datetime.utcnow())
                                memberlist[num] = ([time], True)
                        try:
                                print 'Machine', num, 'joined:', memberlist
                                marshal_memberlist = marshal.dumps(memberlist)
                                sock.sendto(marshal_memberlist, address)
                        except error as e:
                                print 'join error - sending memberlist to new join', e
                        update_pre_suc()
                        pool = ThreadPool(len(memberlist))
                        pool.map(introducer_members, memberlist)
                        pool.close()
                        pool.join
    	sock.close()

def introducer_members(member):
	if member != num:
		d = socket(AF_INET, SOCK_DGRAM)
        serverHost = (MACHINE.format(server_id=member), LISTENJOINPORT)
        try:
                marshal_member = marshal.dumps((num, memberlist[num]))
                d.sendto(marshal_member, serverHost)
                d.close()
        except error as e:
                print 'join error - sending new member to all members', e

#Waits for incoming connections to tcp socket
def main():
	while 1:
		connection, address = s.accept() 
		connection.settimeout(10)
		analyze_data(connection)
		connection.close()

#Creates a thread to handle incoming connections from udp socket for disseminate requests from client while main thread is running
if __name__ == "__main__":
	memberlist[SERVERNAME] = ([str(datetime.datetime.utcnow())], True)
	command_thread = Thread(target = get_command)
	command_thread.daemon = True
	command_thread.start()	
	intro = Thread(target=introducer)
        intro.daemon = True
        intro.start()
	send_hb_thread_pre = Thread(target = send_hb, args = (True, ))
	send_hb_thread_suc = Thread(target = send_hb, args = (False, ))
	wait_hb_thread = Thread(target = wait_hb)
	send_hb_thread_pre.daemon = True
	send_hb_thread_suc.daemon = True
	wait_hb_thread.daemon = True
	send_hb_thread_pre.start()
	send_hb_thread_suc.start()
	wait_hb_thread.start()
	listen_inactive_thread = Thread(target = get_command)
	listen_inactive_thread.daemon = True
	listen_inactive_thread.start(target = listen_inactive())
	main() 

