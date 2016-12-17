from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import os
import logging
import socket
import subprocess
import sys
import threading

class send_file(threading.Thread):  #Uploading to the encoder server
    def __init__(self,server_ip):
        threading.Thread.__init__(self)        
        self.ip = server_ip
    
    def upload_file(self,file_name):
        logging.info("Uploading " + file_name + ".mp4")
        with open(file_name + ".mp4","rb") as handle:
            binary_data =  xmlrpclib.Binary(handle.read())
        subprocess.Popen('rm ' + file_name + ".mp4", stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
        logging.info("Finish uploading...")
        return binary_data
        #return "client got it"

    def run(self):
        server = SimpleXMLRPCServer((self.ip, 10050), logRequests=True)
        server.register_function(self.upload_file)
        logging.info("Start uploading server")
        server.serve_forever()
        
class send_request(threading.Thread):  #Senc uploading request to the encoder server
    def __init__(self,server_name,file_name):
        threading.Thread.__init__(self)
        self.server = server_name
        self.file_name = file_name
    
    def run(self):
        proxy = xmlrpclib.ServerProxy("http://172.22.71.28:10030")
        logging.info("Sending request for file " + self.file_name)
        proxy.upload(self.server,self.file_name)

def log_setting():
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s.%(msecs)03d %(filename)s %(levelname)s: %(message)s',datefmt="%Y-%m-%d %H:%M:%S",filename='Client.log',filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(filename)s %(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


#send_file()

def start_process(server_name,file_name):
    file_name_list = []
    split_dur = 3
    p = subprocess.Popen('./ffmpeg -i ' + file_name, stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    output = p.stderr.read()
    print output
    #print "Duration is " + output[output.find('Duration')+10:output.find('Duration')+18]
    duration = output[output.find('Duration')+10:output.find('Duration')+18].split(':')
    #print "Duation is" + duration[0]+':'+duration[1]+':'+duration[2]
    total_dur_min = int(duration[0]) * 60 + int(duration[1])+(1 if int(duration[2])>0 else 0)
    logging.info("Total Duration is " + str(total_dur_min))
    i = 0
    pre_file_name = file_name.split('.')[0]
    total_pieces = total_dur_min / split_dur + (1 if (total_dur_min%split_dur)>0 else 0)
    logging.info("Total pieces are " + str(total_pieces))
    while i < total_pieces:
        hour = i * split_dur / 60
        minute = i * split_dur % 60
        p = subprocess.Popen('./ffmpeg -ss ' + str(hour) + ':' + str(minute) + ':00 -t 00:' + str(split_dur-1) + ':59 -i ' + file_name + ' -vcodec copy -acodec copy ' + pre_file_name + '_' + str(i+1) + '_' + str(total_pieces) + '.mp4', stdin=subprocess.PIPE, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
        output = p.stderr.read()
        print output
        file_name_list.append(pre_file_name + '_' + str(i+1) + '_' + str(total_pieces))
        i+=1
    #print file_name_list     
    send_file_thread = send_file(server_name)
    send_file_thread.start()
    #proxy = xmlrpclib.ServerProxy("http://172.22.71.28:10000")
    for i in file_name_list:
        upload_file_thread = send_request(server_name,i)
        upload_file_thread.start()

if __name__ == "__main__":
    server_name = socket.gethostbyname(socket.gethostname())  #Get host ip address by host name
    argc = len(sys.argv)
    log_setting()  #Setting the log configuration
    print "Waiting for a call! Press Ctrl+C to exit!"
    if argc == 2:  #Validate the argument
        file_name = sys.argv[1]
        start_process(server_name,file_name)
    else:
        print "Usage: %s FileName" % sys.argv[0]        
