from SimpleXMLRPCServer import SimpleXMLRPCServer

import xmlrpclib
import os
import logging
import threading
import sys
import socket
import Queue
import time
import socket
import subprocess

def log_setting():  #Configure the log setting
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s.%(msecs)03d %(filename)s %(levelname)s: %(message)s',datefmt="%Y-%m-%d %H:%M:%S",filename='Master_Server.log',filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(filename)s %(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

class download_encoded_file(threading.Thread):  #Send the encoded video back to the master server
    def __init__(self,server_ip):
        threading.Thread.__init__(self)
        self.ip = server_ip

    def get_encoded_file(self, file_name):
        with open(file_name,"rb") as handle:
            binary_data =  xmlrpclib.Binary(handle.read())
        subprocess.Popen('rm ' + file_name, stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
        return binary_data

    def run(self):
        server = SimpleXMLRPCServer((self.ip, 10030), logRequests=True)
        server.register_function(self.get_encoded_file)
        logging.info("Start downloading server")
        server.serve_forever()

class heartbeat_server(threading.Thread):  #Receive the heartbeat from the master server
    def __init__(self, server_ip):
        threading.Thread.__init__(self)
        self.ip = server_ip

    def heartbeat_responce(self, heartbeat_info):
        #print "Receive heartbeat:" + heartbeat_info
        return "This is the responce from " + self.ip

    def run(self):
        server = SimpleXMLRPCServer((self.ip, 10066), logRequests=True)
        server.register_function(self.heartbeat_responce)
        logging.info("Start heartbeat server")
        server.serve_forever()

def start_server(server_name, server_port):
    downloader_thread = download_encoded_file(server_name)
    downloader_thread.start()
    heartbeat_thread = heartbeat_server(server_name)
    heartbeat_thread.start()
    uploader_proxy = xmlrpclib.ServerProxy("http://172.22.71.28:10060")
    downloader_proxy = xmlrpclib.ServerProxy("http://172.22.71.28:10090")
    while True:    
        request_res =  uploader_proxy.check_task_available(server_name)
        if request_res != "null":
            logging.info("Got one task: " + request_res)
            pre_file_name = request_res.split('.')[0]
            logging.info("Open file to write")
            with open(request_res,"wb") as handle:
                handle.write(uploader_proxy.upload_file(request_res).data)    
            logging.info("Received file")
            logging.info("Start encoding") 
            p = subprocess.Popen('./ffmpeg -i ' + request_res + ' ' + pre_file_name + '.avi',stdin=subprocess.PIPE, stdout=subprocess.PIPE,shell=True)
            output = p.stdout.read()
            print output
            p = subprocess.Popen('rm ' + pre_file_name + '.mp4',stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
            logging.info("Start sending the file to the master server...")
            logging.info("Master server returns " + downloader_proxy.download_file(server_name,pre_file_name + '.avi'))
            p = subprocess.Popen('rm ' + pre_file_name + '.avi',stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
        else:
            logging.info("No task now")
        time.sleep(2)
        

if __name__ == "__main__":
    server_name = socket.gethostbyname(socket.gethostname())  #Get host ip address by host name
    argc = len(sys.argv)
    log_setting()  #Setting the log configuration
    print "Waiting for a call! Press Ctrl+C to exit!"
    if argc == 2:  #Validate the argument
        server_port = int(sys.argv[1])
        if server_port > 0 and server_port <= 65535:
            start_server(server_name, server_port)
        else:
            print "Invalid ip port!"
            exit(1)
    else:
        print "Usage: %s port" % sys.argv[0]
        exit(1)
