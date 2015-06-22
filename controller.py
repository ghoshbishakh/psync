#!/usr/bin/env python

# Communication Message:
# ------------------------------
# |  Target  | Command  | Data |
# ------------------------------
###
# Target: The module that will process the Data (str)
# Command: Method of the module to be called (str)
# Data: Data that is to be sent
###
# Missing Files Dict:
# fileOD : [fileID, fileName, fileSeq(at remote),
# fileSize, fileSeq(local), peerId]
# sendQueue List:
###         [fileName, fileSeq, address]
###

MAX_RECV_ONGOING = 4
from time import sleep
import time
import logger
from random import shuffle


class controller(object):

    """controls sync operation (initiate file download and upload)

    Arguments:

    communicator - object
    discoverer - object
    fileManager - object

    Methods:
    startSync() - request remote files,
                  update missing files,
                  download missing / incomplete files

    requestRemoteFiles() - calls sendFileList of all peers
    sendFileList(message, address) - sends file list to address
    remoteFileListHandler()
    updateMissingFiles()
    downloadMissingFiles()
    sendFile([fileId. fileSeq], address)
    """

    def __init__(self, communicator, discoverer, fileManager, folderReceiver):

        self.com = communicator
        self.discoverer = discoverer
        self.fileManager = fileManager
        self.allRemoteFiles = {}
        self.missingFiles = {}
        self.sendQueue = []
        self.syncComplete = False
        self.folderReceiver = folderReceiver

    def requestRemoteFiles(self):
        for key, value in self.discoverer.peerList.items():
            peerAddress = (value[0], int(value[1]))
            target = "controller"
            command = "sendFileList"
            message = [target, command, "NONE"]
            self.com.send(message, peerAddress)

    def sendFileList(self, message, address):
        # print self.fileManager.tmpDB
        if(self.discoverer.peerID == "DB"):
            List = []
        else:
            List = self.fileManager.getFiles()
        target = "controller"
        command = "remoteFileListHandler"
        message = [target, command, List]
        self.com.send(message, address)

    def remoteFileListHandler(self, message, address):
        peerID = self.discoverer.getID(address)
        # if(peerID != self.discoverer.peerID):
        self.verifyDest(message)
        self.allRemoteFiles[peerID] = message

    def updateRemoteFiles(self):
        self.requestRemoteFiles()
        for peerId, List in self.allRemoteFiles.items():
            if(peerId in self.discoverer.peerList):
                pass
            else:
                del self.allRemoteFiles[peerId]

    def checkTTL(self, timeStamp, ttl):
        if(ttl == -1):
            return True
        Time = time.time()
        # print Time
        # print ttl
        # print timeStamp
        if(timeStamp + ttl < Time):
            return False
        else:
            return True
    def verifyDest(self, message):
        for fileInfo in message:
            fileDestStatus = fileInfo[8]
            if(fileDestStatus):
                fileID = fileInfo[0]
                if(self.fileManager.fileStatus(fileID)):
                    self.fileManager.setDestStatus(fileID, 1)
                else:
                    self.fileManager.updateEntry(fileID, fileInfo[1:])


    def updateMissingFiles(self):
        # print self.allRemoteFiles
        self.missingFiles = {}
        remoteList = self.allRemoteFiles.items()
        shuffle(remoteList)
        for peerId, List in remoteList:
            if(peerId != self.discoverer.peerID):
                for fileData in List:
                    fileID = fileData[0]
                    fileName = fileData[1]
                    fileSeq = fileData[2]
                    if(fileID not in self.folderReceiver.activeFileId):
                        fileSize = fileData[3]
                        filePriority = fileData[4]
                        timeStamp = fileData[5]
                        ttl = fileData[6]
                        ttlCheck = self.checkTTL(timeStamp, ttl)
                        status = self.fileManager.fileStatus(fileID)
                        fileDestStatus = fileData[8]
                        # print status
                        if(ttlCheck and (not fileDestStatus)):
                            if(status is False):
                                filedata = [fileID, fileName,
                                            fileSeq, fileSize,
                                            filePriority, 0, peerId]
                                if((fileID in self.missingFiles) is False):
                                    self.missingFiles[fileID] = filedata
                                elif((self.missingFiles[fileID][2] != -1) and
                                     (self.missingFiles[fileID][2] < fileSeq)):
                                    self.missingFiles[fileID] = filedata
                            elif(status != -1):
                                filedata = [fileID, fileName,
                                            fileSeq, fileSize,
                                            filePriority, status, peerId]
                                if((fileID in self.missingFiles) is False):
                                    self.missingFiles[fileID] = filedata
                                elif((self.missingFiles[fileID][2] != -1) and
                                     (self.missingFiles[fileID][2] < fileSeq)):
                                    self.missingFiles[fileID] = filedata
                            else:
                                if((fileID in self.missingFiles) is True):
                                    del self.missingFiles[fileID]

    def downloadMissingFiles(self):
        target = "controller"
        command = "sendFile"
        for fileId, fileData in self.missingFiles.items():
            fileSeq = fileData[5]  # ------ local file sequence
            message = [target, command, [fileId, fileSeq]]
            address = self.discoverer.getAddress(fileData[6])
            self.com.send(message, address)
            # print message

    def downloadMissingFilesHP(self):
        priorityList = []
        ongoing = len(self.folderReceiver.activeFileId)
        i = 0
        target = "controller"
        command = "sendFile"
        for fileId, fileData in self.missingFiles.items():
            filePriority = fileData[4]
            priorityList.append(filePriority)
        if(priorityList):
            priorityList.sort()
            HP = priorityList[-1]
            fileList = self.missingFiles.items()
            shuffle(fileList)
            for fileId, fileData in fileList:
                fileSeq = fileData[5]  # ------ local file sequence
                filePriority = fileData[4]
                if((filePriority >= HP) and (ongoing <= MAX_RECV_ONGOING)):
                    message = [target, command, [fileId, fileSeq]]
                    address = self.discoverer.getAddress(fileData[6])
                    self.com.send(message, address)
                    i += 1
                    ongoing += 1
                    # print message

    def sendFile(self, fileInfo, address):
        fileID = fileInfo[0]
        fileSeq = fileInfo[1]
        send = [fileID, fileSeq, address]
        peerID = self.discoverer.getID(address)
        # print "sending: " + str(send)
        if((send in self.sendQueue) is False):
            if((self.discoverer.dbPresent()) and (peerID == "DB")):
                self.sendQueue.append(send)
            elif(not self.discoverer.dbPresent()):
                self.sendQueue.append(send)

    def showMissingFiles(self):
        print "\n---------------------------------------"
        print "MISSING FILES"
        for fileId, fileData in self.missingFiles.items():
            name = fileData[1]
            print name

    def startSync(self):
        self.updateRemoteFiles()
        sleep(1)
        self.updateMissingFiles()
        print self.showMissingFiles()
        if(len(self.missingFiles) == 0 and self.syncComplete is False):
            logList = [str(time.time()), "MISSING FILES NONE"]
            logger.log(str(logList))
            self.syncComplete = True
        elif(len(self.missingFiles) > 0):
            self.syncComplete = False
        self.downloadMissingFilesHP()
