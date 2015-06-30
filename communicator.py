#! /usr/bin/env python
from threading import Thread
import pickle
import socket
import time

class communicator(object):

    """communicator: Send and Receive Message.

    Arguments:

    handler - called when data is received
    args:(target, command, message, address)

    address - ip address for communicaton socket to bind
    (type: str, default: '127.0.0.1')

    port - port used for communicaton socket (type: int, default: 55555)
    """

    def __init__(self, handler, address='127.0.0.1', port=55555):
        self.address = address
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.3)
        self.queue = []
        self.ackQueue = []
        # bind to ADDR
        try:
            self.sock.bind((self.address, self.port))
            print "Communication Socket created. \n"
        except:
            print "Could not create socket at " + str(self.address)
            raise
        self.controlRouter = handler

    def listener(self):
        """Start listening for messages and call comHandler"""
        # print "running com listener"
        while(1):
            # print "receiving"
            try:
                data, address = self.sock.recvfrom(65507)
                # print "thread receive: "+str(data) + "\n"
                if("***" in data):
                    data = data.split("***")
                    self.comHandler(data[0], address)
                    self.comHandler(data[1], address)
                else:
                    self.comHandler(data, address)
            except:
                pass

    def comHandler(self, dataPickle, address):
        """parse data from pickle Message, and call handler with
        Arguments:
        pickleMessage - the message received from socket
        address - address of sender of the message
        Message must contain:
        target - target module to be called by handler
        command - method of target to be called
        data - data that is passed to the method
        """
        data = pickle.loads(dataPickle)
        target = data[0]
        command = data[1]
        message = data[2]
        # print data
        self.controlRouter(target, command, message, address)

    def send(self, data, addr):
        """convert data to message to pickleMessage and send to address
        Arguments:
        message - list with target(str), command(str), data(list)
        address - touple - (IP, PORT)  (IP: str and PORT: int)
        """
        dataPickle = pickle.dumps(data, 2)
        # print addr
        frame = [dataPickle, addr]
        if(frame in self.queue):
            pass
        else:
            self.queue.append(frame)

    def sendAck(self, data, addr):
        dataPickle = pickle.dumps(data, 2)
        frame = (dataPickle, addr)
        if(frame in self.ackQueue):
            pass
        else:
            self.ackQueue.append(frame)

    def piggyback(self):
        for ack, addr in self.ackQueue:
            piggybacked = False
            for dataFrame in self.queue:
                if((len(dataFrame) < 3) and (type(dataFrame) != tuple) and (dataFrame[1] == addr)):
                    self.queue[self.queue.index(dataFrame)].insert(0, ack)
                    self.ackQueue.remove((ack, addr))
                    piggybacked = True
                    break
            if(not piggybacked):
                self.queue.append((ack, addr))
                self.ackQueue.remove((ack, addr))

    def frameSender(self):
        while 1:
            self.piggyback()
            if(self.queue):
                frame = self.queue[0]
                if(len(frame) > 2):
                    dataPickle = frame[0] + "***" + frame[1]
                    addr = frame[2]
                else:
                    dataPickle = frame[0]
                    addr = frame[1]
                try:
                    self.sock.sendto(dataPickle, addr)
                except:
                    print "NETWORK UNREACHABLE"
                    print "\n **** PLEASE CONNECT TO NETWORK **** \n"
                del self.queue[0]
            time.sleep(0.000001)

    def run(self):
        """Start listening for messages and call comHandler
        Runs in a separate thread of control. Thread exits silently
        when main program exits.
        """
        listenerThread = Thread(target=self.listener, args=())
        listenerThread.setDaemon(True)
        listenerThread.start()

        frameSenderThread = Thread(target=self.frameSender, args=())
        frameSenderThread.setDaemon(True)
        frameSenderThread.start()
