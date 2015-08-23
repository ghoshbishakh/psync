#!/usr/bin/env python

# ##     Presently it uses BROADCASTING
# ##     Broadcast Message Format:
# ##             -------------------------------------------------------------------
# ##             |  PEER ID  |  PEER IP  | PEER PORT | READY TO CONNECT (y=1/n=0)  |
# ##             -------------------------------------------------------------------
# ##
# ##     peerList: (Dictionary)
# ##             {'peerId':[peerIP, peerPORT, timeout]}
# ##
# ##               timeout: is seconds elapsed after last broadcast received
# ##

import time
from datetime import datetime
import socket
import json
from time import sleep
from threading import Thread
import logger

class discoverer(object):
    """discovers nodes on same network

    Arguments:

    peerIP - IP address of the communicator
    peerPORT - PORT used by communicator
    handler - called when peer appears or disappears (peerId, data, 0/1)
    0 = Disconnect, 1 = connect
    args: (peerId, peerData)
    broadcasrIP - (default:'<broadcast>')
    broadcastPORT (default:55554)
    interval - time between broadcasts (seconds)
    timeout - time (seconds) for peer expir after last broadcast

    Methods:

    startDiscovery() - starts the entire discovery process
    getID(address) - get peerID from addtess(touple)
    getAddress(peerID) - get address(touple) from peerID
    """
    def __init__(self, peerIP, peerPORT, peerID, handler,
                 broadcastIP='<broadcast>', broadcastPORT=55554,
                 interval=.5, timeout=10):
        self.broadcastIP = broadcastIP
        self.broadcastPORT = broadcastPORT
        self.broadcastAddr = (self.broadcastIP, broadcastPORT)
        self.peerIP = peerIP
        self.peerPORT = peerPORT
        self.peerID = peerID
        self.peerHandler = handler
        self.interval = interval
        self.timeout = timeout
        self.peerList = {}
        self.readyToConnect = 1

#========================= CREATE SOCKET ============================
        self.broadcastSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcastSock.setsockopt(
            socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        print "Discoverer Socket created at. \n" + str(self.broadcastAddr)
        logStr = "['"+str(time.time())+"', 'START_DISCOVERY']"
        logger.log(logStr)

#======================== CHANGE READY TO CONNECT STATE =============
    def ready(self, state):
        self.readyToConnect = int(state)

#========================== BROADCAST ========================================

    def broadcast(self):
        count = 1
        #print "Broadcasting ..."
        while 1:
            try:
                message = [self.peerID, self.peerIP, self.peerPORT, str(count), str(datetime.now().strftime("%H:%M:%S:%f")), self.readyToConnect]
                messageJson = json.dumps(message)
                self.broadcastSock.sendto(messageJson, self.broadcastAddr)
                count += 1
                #print messageJson
            except:
                print "BROADCAST FAILED - NETWORK UNREACHABLE"
            #print "broadcasting"
            sleep(self.interval)

    def startBroadcast(self):
        self.BroadcastThread = Thread(target=self.broadcast, args=())
        self.BroadcastThread.setDaemon(True)
        self.BroadcastThread.start()

#========================= LISTEN =============================================

    def listenerHandler(self, message):
        peerID = str(message[0])
        peerIP = str(message[1])
        peerPORT = str(message[2])
        Count = str(message[3])
        senderTime = str(message[4])
        readyToConnect = str(message[5])
        if(peerID != self.peerID):
            logStr = "['"+str(datetime.now().strftime("%H:%M:%S:%f"))+"', 'BROADCAST_RECEIVED','"+peerID+"','"+peerIP+"','"+peerPORT+"','"+Count+"','"+senderTime+"','"+readyToConnect+"']"
            logger.log(logStr)
            #print message
        timeout = 0
        peerData = [peerIP, peerPORT, timeout, readyToConnect]
        if((peerID in self.peerList) and
          (self.peerList[peerID][:-2] == peerData[:-2])):
            self.peerList[peerID] = peerData
            self.peerList[peerID][2] = 0
        else:
            self.peerList[peerID] = peerData
            try:
                self.peerHandler(peerID, peerData, 1)
            except:
                print "newPeer handler Error - discoverer module"
            else:
                pass

    def listener(self):
        self.listenerSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listenerSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listenerSock.bind(self.broadcastAddr)
        #print "receiving"
        while 1:
            messageJson, addr = self.listenerSock.recvfrom(10240)
            #print messageJson
            message = json.loads(messageJson)
            #print "received"
            self.listenerHandler(message)
        #print "JAH!"

    def startListener(self):
        self.listenThread = Thread(target=self.listener, args=())
        self.listenThread.setDaemon(True)
        self.listenThread.start()

#========================= PEER EXPIRY =======================================
    def lapser(self):
            while 1:
                for key, value in self.peerList.items():
                    value[2] += 1
                    if(value[2] >= self.timeout):
                        try:
                            self.peerHandler(key, value, 0)
                        except:
                            print "peerDisconnect handler Error - discoverer"
                        else:
                            pass
                        del self.peerList[key]
                sleep(1)

    def startLapser(self):
        self.lapserThread = Thread(target=self.lapser, args=())
        self.lapserThread.setDaemon(True)
        self.lapserThread.start()

#======================= RUN DISCOVERY ======================================
    def startDiscovery(self):
        self.startBroadcast()
        self.startListener()
        self.startLapser()
        print "Discovery started"

#================== GET PEER INFO ==========================================

    def getID(self, addr):
        peerID = None
        for key, value in self.peerList.items():
            if((value[0], int(value[1])) == addr):
                peerID = key
                break
            else:
                pass
        return peerID

    def getAddress(self, peerID):
        address = None
        for key, value in self.peerList.items():
            if(key == peerID):
                address = (value[0], int(value[1]))
                break
            else:
                pass
        return address

    def dbPresent(self):
        for key, value in self.peerList.items():
            if(key == "DB"):
                return True
        return False
