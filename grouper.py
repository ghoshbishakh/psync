#!/usr/bin/env python
import time
import random
from threading import Thread
import logger

# connectedPeerList {"peerID":[peerIP, peerPORT, timeout, readyToConnect, connectionDuration]}

class grouper(object):
    """handles grouping of nodes discovered by discoverer"""
    def __init__(self, discoverer, communicator, controlRouter, maxPeers, maxDuration):
        self.discoverer = discoverer
        self.com = communicator
        self.controlRouter = controlRouter
        self.allPeerList = self.discoverer.peerList
        self.maxPeers = maxPeers
        self.maxDuration = maxDuration
        self.readyToConnectPeerList = {}
        self.connectedPeerList = {}
        self.expiredPeers = []
        self.validConnections = 0

    def updateReadyToConPeers(self):
        self.updateReadyToCon = True
        while(self.updateReadyToCon):
            print "allPeerList=" + str(self.allPeerList)
            print "readyToConnect=" + str(self.readyToConnectPeerList)
            print "connectedPeerList=" + str(self.connectedPeerList)
            print "ExpiredPeerList=" + str(self.expiredPeers)
            for peerId, peerData in self.allPeerList.items():
                if(int(peerData[3]) == 1):
                    inConnectedList = False
                    for peerId2, peerData2 in self.connectedPeerList.items():
                        if(peerId == peerId2):
                            inConnectedList = True
                    if(not inConnectedList):
                        self.readyToConnectPeerList[peerId] = peerData
            for peerId, peerData in self.readyToConnectPeerList.items():
                stillReady = False
                for peerId2, peerData2 in self.allPeerList.items():
                    if(peerId == peerId2):
                        if((int(self.allPeerList[peerId2][3]) == 1) and (peerId not in self.connectedPeerList)):
                            stillReady = True
                if(not stillReady):
                    del self.readyToConnectPeerList[peerId]
            time.sleep(1)

    def updateConnections(self):
        self.updateCon = True
        while(self.updateCon):
            #Increase connection time and maintain expired peer list
            for peerId, peerData in self.connectedPeerList.items():
                self.connectedPeerList[peerId][4] += 1
                if(peerData[4]>self.maxDuration):
                    if(peerId in self.expiredPeers):
                        pass
                    else:
                        self.expiredPeers.append(peerId)
                isAvailable = False
                for peerId2, peerData2 in self.allPeerList.items():
                    if(peerId2 == peerId):
                        isAvailable = True
                if(not isAvailable):
                    del self.connectedPeerList[peerId]
                    logStr = "['"+str(time.time())+"', 'REMOVE_FROM_GROUP', '"+str(peerId)+"', '"+str((peerData[0], int(peerData[1])))+"']"
                    logger.log(logStr)
                    print logStr

            for peer in self.expiredPeers:
                connected = False
                for peerId, peerData in self.connectedPeerList.items():
                    if(peerId == peer):
                        connected = True
                if(not connected):
                    self.expiredPeers.remove(peer)

            self.validConnections = len(self.connectedPeerList)-len(self.expiredPeers)
            if(self.validConnections<self.maxPeers):
                self.discoverer.ready(1)
            else:
                self.discoverer.ready(0)

            self.shuffleConn()
            time.sleep(1)


    def shuffleConn(self):
        if((self.validConnections<self.maxPeers) and len(self.readyToConnectPeerList)):
            i = 0
            print "shuffle"
            random.shuffle(self.readyToConnectPeerList.items())
            for peerId, peerData in self.readyToConnectPeerList.items():
                if((self.validConnections+i)<self.maxPeers):
                    self.requestConnection((peerData[0], int(peerData[1])))
                    #print "request", peerId
                    i+=1

    def requestConnection(self, peerAddress):
        target = "grouper"
        command = "receiveConnection"
        message = [target, command, self.discoverer.peerID]
        self.com.send(message, peerAddress)

    def receiveConnection(self, message, address):
        peerId = message
        #print "++++++++>>", self.connectedPeerList
        if(peerId in self.connectedPeerList):
            self.sendConfirmation(address)
            #print "---Already present--> confirm"
        elif(self.validConnections<self.maxPeers):
            self.registerConnection(peerId)
            logStr = "['"+str(time.time())+"', 'ADD_TO_GROUP', '"+str(peerId)+"', '"+str(address)+"']"
            logger.log(logStr)
            print logStr
            self.sendConfirmation(address)

    def sendConfirmation(self, address):
        target = "grouper"
        command = "receiveConfirmation"
        message = [target, command, self.discoverer.peerID]
        self.com.send(message, address)

    def receiveConfirmation(self, message, address):
        self.registerConnection(message)

    def registerConnection(self, peerId):
        if((peerId in self.allPeerList) and (peerId not in self.connectedPeerList) and (self.validConnections<self.maxPeers)):
            peerData = self.allPeerList[peerId]
            peerData += [0]
            self.connectedPeerList[peerId] = peerData
            removed = self.removeOldestCon()
            if(removed):
                print "added and removed connection successfully"
            else:
                print "added connection but not removed any"

    def removeOldestCon(self):
        removed = False
        for peer in self.expiredPeers:
            if(peer in self.connectedPeerList):
                address = (self.connectedPeerList[peer][0],self.connectedPeerList[peer][1])
                self.removeConnRequest(peer)
                #self.controlRouter("fileSenderManager", "stopTransmission", address, address)
                del self.connectedPeerList[peer]
                removed = True
                break
        return removed

    def removeConnRequest(self, peerId):
        peerAddress = (self.connectedPeerList[peerId][0], int(self.connectedPeerList[peerId][1]))
        target = "grouper"
        command = "removeConn"
        message = [target, command, self.discoverer.peerID]
        self.com.send(message, peerAddress)

    def removeConn(self, message, address):
        peerId = message[2]
        if(peerId in self.connectedPeerList):
            del self.connectedPeerList[peerId]

    def start(self):
        updateConnectionsThread = Thread(target=self.updateConnections, args=())
        updateConnectionsThread.setDaemon(True)
        updateConnectionsThread.start()

        updateReadyToConPeersThread = Thread(target=self.updateReadyToConPeers, args=())
        updateReadyToConPeersThread.setDaemon(True)
        updateReadyToConPeersThread.start()

    def stop(self):
        self.updateCon = False
        self.updateReadyToCon = False
