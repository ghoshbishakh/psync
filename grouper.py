#!/usr/bin/env python
import time
import random

# connectedPeerList {"peerID":[peerIP, peerPORT, timeout, readyToConnect, connectionDuration]}

class grouper(object):
    """handles grouping of nodes discovered by discoverer"""
    def __init__(self, discoverer, communicator, maxPeers, maxDuration):
        self.discoverer = discoverer
        self.com = communicator
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
            for peerId, peerData in self.allPeerList.items():
                if(peerData[3] == 1):
                    inConnectedList = False
                    for peerId2, peerData2 in self.connectedPeerList.items():
                        if(peerId == peerId2):
                            inConnectedListList = True
                    if(not inConnectedListList):
                        self.readyToConnectPeerList[peerId] = peerData
            for peerId, peerData in self.readyToConnectPeerList.items():
                stillReady = False
                for peerId2, peerData2 in self.allPeerList.items():
                    if(peerId == peerId2):
                        if((self.allPeerList[peerId2][3] == 1) and (peerId not in self.connectedPeerList)):
                            stillReady = True
                if(not stillReady):
                    del self.readyToConnectPeerList[peerId]
            time.sleep(1)

    def updateConnections(self):
        self.updateCon = True
        while(self.updateCon):
            #Increase connection time and maintain expired peer list
            for peerId, peerData in self.connectedPeerList.items():
                connectedPeerList[peerId][4] += 1
                if(peerData[4]>self.maxDuration):
                    if(peerId in self.expiredPeers):
                        pass
                    else:
                        self.expiredPeers.append(peerId)
            for peer in self.expiredPeers.items():
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
        if((validConnections<self.maxPeers) and len(self.readyToConnectPeerList)):
            i = 0
            random.shuffle(self.readyToConnectPeerList.items())
            for peerId, peerData in self.readyToConnectPeerList.items():
                if((self.validConnections+i)<self.maxPeers):
                    self.requestConnection((peerData[0], int(peerData[1])))
                    i+=1

    def requestConnection(self, peerAddress):
        target = "grouper"
        command = "receiveConnection"
        message = [target, command, self.discoverer.peerID]
        self.com.send(message, peerAddress)

    def receiveConnection(self, message, address):
        peerId = message[2]
        if(peerId in self.connectedPeerList):
            sendConfirmation(peerId, message , address)
        elif(self.validConnections<self.maxPeers):
            self.registerConnection(peerId)
            self.sendConfirmation(address)

    def sendConfirmation(self, address):
        target = "grouper"
        command = "receiveConfirmation"
        message = [target, command, self.discoverer.peerID]
        self.com.send(message, peerAddress)

    def receiveConfirmation(self, message, address):
        self.registerConnection(message[2])

    def registerConnection(self, peerId):
        if(peerId in self.allPeerList):
            peerData = self.allPeerList[peerId]
            peerData += [0]
            self.connectedPeerList[peerId] = peerData
            removed = self.removeOldestCon()
            if(removed):
                print "added and removed connection successfully"
            else:
                "added connection but not removed any"

    def removeOldestCon(self):
        removed = False
        for peer in self.expiredPeers.items():
            if(peer in self.connectedPeerList):
                self.removeConnRequest(peer)
                del self.connectedPeerList[peer]
                removed = True
                break
        return remove

    def removeConnRequest(self, peerId):
        peerAddress = (self.connectedPeerList[peerId][0], int(self.connectedPeerList[peerId][1])
        target = "grouper"
        command = "removeConn"
        message = [target, command, self.discoverer.peerID]
        self.com.send(message, peerAddress)

    def removeConn(self, message, address):
        peerId = message[2]
        if(peerId in self.connectedPeerList):
            del self.connectedPeerList[peerId]
