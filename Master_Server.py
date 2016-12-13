from SimpleXMLRPCServer import SimpleXMLRPCServer

import xmlrpclib
import os
import logging
import threading
import sys
import socket
import Queue
import subprocess

nameToTotalPieces = {}
mutex = threading.Lock()
task_queue = Queue.Queue(0)
#encoder_list = {}

class task_upload(threading.Thread):
    def __init__(self,server_ip):
        threading.Thread.__init__(self)
        self.ip = server_ip

    def check_task_available(self):
        if task_queue.empty():
            return "null"
        else:
            if mutex.acquire(1):
                file_name = task_queue.get()
                mutex.release()
                return file_name

    def upload_file(self, file_name):
        with open(file_name,"rb") as handle:
            binary_data =  xmlrpclib.Binary(handle.read())
        subprocess.Popen('rm ' + file_name, stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)    
        return binary_data

        server = SimpleXMLRPCServer((self.ip, 10060), logRequests=True)
        server.register_function(self.check_task_available)
        server.register_function(self.upload_file)
        logging.info("Uploading server on...")
        server.serve_forever()

class uploader(threading.Thread):
    def __init__(self,server_ip):
        threading.Thread.__init__(self)
        self.ip = server_ip

    def upload(self,client_ip, file_name):
        print file_name
        file_info = file_name.split('_')
        if nameToTotalPieces.has_key(file_info[0]) == False:
            nameToTotalPieces[file_info[0]] = int(file_info[2])
        print nameToTotalPieces
        proxy = xmlrpclib.ServerProxy("http://" + client_ip + ":10050")
        with open(file_name + ".mp4","wb") as handle:
            handle.write(proxy.upload_file(file_name).data)
        if mutex.acquire(1):
            task_queue.put(file_name + ".mp4")
            mutex.release()
        print task_queue
        #print proxy.upload_file(file_name)
        return "Got it" + file_name

    def run(self):
        server = SimpleXMLRPCServer((self.ip, 10030), logRequests=True)
        server.register_function(self.upload)
        logging.info("Start receiving server")
        server.serve_forever()

class downloader(threading.Thread):
    def __init__(self, server_ip):
        threading.Thread.__init__(self)
        self.ip = server_ip
    
    def download_file(self, encoder_ip, file_name):
        print file_name + " complete encoding. Start downloading..."
        proxy = xmlrpclib.ServerProxy("http://" + encoder_ip + ":10030")
        with open(file_name,"wb") as handle:
            handle.write(proxy.get_encoded_file(file_name).data)
        original_file_name = file_name.split('.')[0].split('_')[0]
        total_pieces = nameToTotalPieces[original_file_name]
        is_all_encoded = True
        for i in range(1,total_pieces+1):
            if os.path.isfile(original_file_name + '_' + str(i) + '_' + str(total_pieces) + '.avi') == False:
                is_all_encoded = False
                break
        if is_all_encoded == False:
            print "Lack some pieces!"
        else:
            concat_command = './ffmpeg -i \"concat:'
            for i in range(1, total_pieces+1):
                if i == total_pieces:
                    concat_command = concat_command + original_file_name + '_' + str(i) + '_' + str(total_pieces) + '.avi\" -c copy ' + original_file_name + '.avi'
                else:
                    concat_command = concat_command + original_file_name + '_' + str(i) + '_' + str(total_pieces) + '.avi|'
            print concat_command
            p = subprocess.Popen(concat_command,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
            for i in range(1, total_pieces+1):
                p = subprocess.Popen('rm ' + original_file_name + '_' + str(i) + '_' + str(total_pieces) + '.avi',stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
            print "Task:" + original_file_name + ".mp4 to " + original_file_name + ".avi finished"
        return file_name + " Received"
            
    def run(self):
        server = SimpleXMLRPCServer((self.ip, 10090), logRequests=True)
        server.register_function(self.download_file)
        logging.info("Start downloader server")
        server.serve_forever()       

class heartbeat_server(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        

def start_server(server_name,server_port):
    uploader_thread = uploader(server_name)
    uploader_thread.start()
    task_upload_thread = task_upload(server_name)
    task_upload_thread.start()
    downloader_thread = downloader(server_name)
    downloader_thread.start()

def log_setting():
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s.%(msecs)03d %(filename)s %(levelname)s: %(message)s',datefmt="%Y-%m-%d %H:%M:%S",filename='Master_Server.log',filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(filename)s %(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

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
