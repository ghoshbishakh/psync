#!/usr/bin/env python

import os
import sys
import base64
from time import sleep
import time
from threading import Thread
import hashlib
import logger


# define packet length and address of socket and window size------------------
LENGTH = 1000
W = 32


# HANDLING ACKs---------------------------------------------------------------
class ackTracker(object):

    """holds all acks nacks and handles listeners data"""

    def __init__(self, senderThread):
        self.stop = False
        self.acceptRESET = False
        self.acks = {}
        self.offset = 0
        self.address = None
        self.c = None
        self.sendReset = None
        self.fileName = None

    def handler(self, data):
        # print self.acks
        Data = data
        fileName = Data[1]
        for sqstatus in Data[2]:
            sequence = sqstatus[0]
            #print "ackRecv: " + str(sequence) + str(Data[3])
            # if(type(sequence) == int):
            #     sequence = sequence - self.offset
            status = sqstatus[1]

            if((sequence == "RESET") and (self.acceptRESET is True)):
                # print "DONE!! SENDING"
                self.stop = True
            if((sequence == "FINISH") and (self.acceptRESET is True)):
                # print "DONE!! SENDING"
                self.stop = True
            if((sequence == "STOP") and (self.acceptRESET is True)):
                # print "DONE!! SENDING"
                self.stop = True
            elif(sequence in self.acks):
                if(self.acks[sequence] == 'y'):
                    pass
                else:
                    self.acks[sequence] = status
            else:
                self.acks[sequence] = status
                # print str(sequence) + ": " + str(self.acks[sequence])
                # print self.acks
                # print "\n"

    def checkAck(self, sequence, size):
        if(sequence in self.acks):
            # print self.acks, sequence
            if(self.acks[sequence] == 'y'):
                return True
            elif(self.acks[sequence] == 'END'):
                self.sendReset(self.fileName, self.c, self.address)
            # print "reset \n"
        else:
            return False


# File Sender Thread ------------------------------------------------------
class fileSenderThread(object):

    """file sender thread"""

    def __init__(self, fileManager, fileID, filePath, address, fileSeq, filePriority,
                 timeStamp, ttl, fileDest, fileDestStatus, communicator, SendId, onGoing):
        self.fileManager = fileManager
        self.fileID = fileID
        self.fileStatus = self.fileManager.fileStatus(self.fileID)
        if(self.fileStatus == -1):
            self.fullFile = True
        else:
            self.fullFile = False
        self.filePath = filePath
        self.address = address
        self.fileSeq = fileSeq
        self.filePriority = filePriority
        self.timeStamp = timeStamp
        self.ttl = ttl
        self.fileDest = fileDest
        self.fileDestStatus = fileDestStatus
        self.c = communicator
        self.onGoing = onGoing
        self.SendId = SendId
        self.tracker = ackTracker(self)
        self.progress = None
        self.resendCount = 0
        self.resendGap = .0001
        self.sendGap = .0001

    # PROGRESS --------------------------------------------------------------
    def updateProgress(self, sequence, size):
        sentSize = sequence * LENGTH
        totalSize = size
        percentage = float(sentSize) / float(totalSize) * 100.0
        self.progress = percentage
        sys.stdout.write("\r%f%%" % percentage)
        sys.stdout.flush()

    # File Sending ----------------------------------------------------------
    def sendReset(self, fileName, c, address):
        target = "folderReceiver"
        command = "receive"
        if(self.fullFile):
            status = "FINISH"
        else:
            status = "STOP"
        data = [self.fileID, fileName, status, "", 0, self.filePriority]
        message = [target, command, data]
        c.send(message, address)
        self.tracker.acceptRESET = True
        # print "RESETTING"

    def endTransmission(self, fileID, filename, sequence, c, address):
        target = "folderReceiver"
        command = "receive"
        chunkData = "END"
        chunkString = chunkData
        data = [fileID, filename, sequence, chunkString, 0, self.filePriority]
        message = [target, command, data]
        c.send(message, address)
        # print chunkData
        # sys.exit("FINISHED!!! \n")

    def send(self, fileObj, fileID, filename, filesize, sequence, c, address):
        seekPosition = LENGTH * sequence
        # print fileObj
        fileObj.seek(seekPosition)
        chunkData = fileObj.read(LENGTH)
        # print chunkData
        if(chunkData == ''):
            # print "sending END"
            self.endTransmission(fileID, filename, sequence, c, address)
        else:
            chunkString = chunkData
            if(sequence == self.fileSeq):
                data = [fileID, filename, sequence, chunkString, filesize,
                        self.filePriority, self.timeStamp, self.ttl, self.fileDest, self.fileDestStatus]
                # print "first: "
            else:
                data = [fileID, filename, sequence, chunkString, filesize]
            # print data
            target = "folderReceiver"
            command = "receive"
            message = [target, command, data]
            c.send(message, address)
            # print "sending" + str(sequence)
            # print "sending:----"
            # print sequence
        sleep(self.sendGap * self.delay)
        # print str(sequence)+" sent \n"
        # print "delay: " + str(self.delay)

    def sendFile(self, filePath, fileSeq, address, c):
        path = filePath
        try:
            fileObj = open(str(path), 'r')
            path, fileName = os.path.split(fileObj.name)
        except:
            print "could not open file"
            return False
        fileObj.seek(0, os.SEEK_END)
        fileSize = fileObj.tell()
        fileObj.seek(0)
        print "\n--------------  SENDING   -------------------\n"
        print "FILE NAME: " + fileName
        print "FILE SIZE: " + str(float(fileSize) / 1024.0) + "KB"
        print "FILE PRIORITY: " + str(self.filePriority)
        print "FROM SEQUENCE: " + str(fileSeq)
        if(fileSeq == 0):
            logList = [str(time.time()), "SENDING", str(fileName),
                       str(float(fileSize) / 1024.0),
                       str(address), str(fileSeq)]
        #    logTxt = "SENDING " + \
        #        str(fileName) + " " + str(float(fileSize) / 1024.0) + \
        #        " KB " + " to " + str(address) + " from: " + str(fileSeq)
        else:
            logList = [str(time.time()), "RESUMING", str(fileName),
                       str(float(fileSize) / 1024.0),
                       str(address), str(fileSeq)]
        #  logTxt = "RESUMING " + \
        #      str(fileName) + " " + str(float(fileSize) / 1024.0) + \
        #      " KB " + " to " + str(address) + " from: " + str(fileSeq)

        logger.log(str(logList))
        sequence = fileSeq
        lastAckCheck = sequence - 1
        LFS = sequence - 1
        self.tracker.stop = False
        self.delay = 500
        self.tracker.address = address
        self.tracker.c = c
        self.tracker.acceptRESET = False
        self.tracker.acks = {}
        self.tracker.offset = sequence
        self.tracker.fileName = fileName
        self.tracker.sendReset = self.sendReset
        if(self.fullFile):
            status = "FINISHED SENDING"
        else:
            status = "FINISHED SENDING PART"
        while(self.tracker.stop is False):
            # print "resend:", self.fileID, self.resendCount
            if(self.delay > 1000):
                    self.delay = 1
            if(self.resendCount > 500):
                status = "FAILED SENDING"
                break
            if((LFS - lastAckCheck) < W):
                # print "SENDING" + str(sequence)
                self.send(fileObj, self.fileID, fileName,
                          fileSize, sequence, c, address)
                LFS = sequence
                sequence += 1
                self.resendCount = 0
                if(self.delay > 1):
                    self.delay -= self.delay / 100.0

            else:
                if(self.tracker.checkAck((lastAckCheck + 1), fileSize)):
                    lastAckCheck += 1
                    # print "Checked: " + str(lastAckCheck)
                else:
                    sleep(self.resendGap * self.delay)
                    # print "resend: " + str(lastAckCheck + 1)
                    self.send(fileObj, self.fileID, fileName,
                              fileSize, (lastAckCheck + 1), c, address)
                    self.resendCount += 1
                    self.delay += 1
                    # print self.resendCount
        fileObj.close()
        del self.onGoing[self.SendId]
        print "\n-------------- " + status + "   -------------------\n"
        print "FILE NAME: " + fileName
        # print "FILE SIZE: " + str(float(fileSize) / 1024.0) + "KB"
        if(status == "FINISHED SENDING"):
            self.fileManager.checkDest(self.fileID, self.address)
        logList = [str(time.time()), str(status), str(fileName),
                   str(float(fileSize) / 1024.0),
                   str(address)]
        # logTxt = status + ": " + \
        #     str(fileName) + " " + str(float(fileSize) / 1024.0) + \
        #     " KB " + " to " + str(address)
        logger.log(str(logList))
        return True

    def run(self):
        # print "*************************"
        # print self.filePath
        # print self.fileSeq
        self.thread = Thread(
            target=self.sendFile, args=(self.filePath, self.fileSeq,
                                        self.address, self.c))
        self.thread.setDaemon(True)
        self.thread.start()


# File Sender Manager -------------------------------------------------------
class fileSenderManager(object):

    """file Sender """

    def __init__(self, fileManager):
        self.onGoing = {}
        self.fileManager = fileManager

    def generateSendId(self, fileID, address):
        ip = str(address[0])
        port = str(address[1])
        hashString = (fileID + ip + port)
        SendId = hashlib.sha1(hashString).hexdigest()
        return SendId

    def sendFile(self, fileName, fileID, filePath, fileSeq, filePriority,
                 timeStamp, ttl, fileDest, fileDestStatus, address, communicator):
        SendId = self.generateSendId(fileID, address)
        if(SendId in self.onGoing):
            pass
        else:
            self.onGoing[SendId] = fileSenderThread(
                self.fileManager, fileID, filePath, address, fileSeq, filePriority,
                timeStamp, ttl, fileDest, fileDestStatus, communicator, SendId, self.onGoing)
            self.onGoing[SendId].run()

    def handleACK(self, data, address):
        fileName = data[1]
        fileID = data[0]
        # print fileName
        # print data[1]
        fileId = self.generateSendId(fileID, address)
        if(fileId in self.onGoing):
            self.onGoing[fileId].tracker.handler(data)
        else:
            pass
            # print "DHAAAAT"
