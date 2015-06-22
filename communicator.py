#! /usr/bin/env python
from threading import Thread
import json
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
                self.comHandler(data, address)
            except:
                pass

    def comHandler(self, dataJson, address):
        """parse data from json Message, and call handler with
        Arguments:
        jsonMessage - the message received from socket
        address - address of sender of the message
        Message must contain:
        target - target module to be called by handler
        command - method of target to be called
        data - data that is passed to the method
        """
        data = json.loads(dataJson)
        target = data[0]
        command = data[1]
        message = data[2]
        # print data
        self.controlRouter(target, command, message, address)

    def send(self, data, addr):
        """convert data to message to jsonMessage and send to address
        Arguments:
        message - list with target(str), command(str), data(list)
        address - touple - (IP, PORT)  (IP: str and PORT: int)
        """
        dataJson = json.dumps(data)
        # print addr
        frame = (dataJson, addr)
        if(frame in self.queue):
            pass
        else:
            self.queue.append(frame)

    def frameSender(self):
        while 1:
            if(self.queue):
                frame = self.queue[0]
                dataJson = frame[0]
                addr = frame[1]
                try:
                    self.sock.sendto(dataJson, addr)
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