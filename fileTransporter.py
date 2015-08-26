#! /usr/bin/env python
from time import sleep
from time import time
import libSend
import os
from threading import Thread
import hashlib
import logger


LENGTH = 1000
W = 32
MAX_SEND_ONGOING = 4

class fileTransporter(object):

    """sends file and handles file reception"""

    def __init__(self, controller, folderPath, communicator, fileManager):
        self.queue = controller.sendQueue
        self.folder = folderPath
        self.communicator = communicator
        self.fileManager = fileManager
        self.fileSenderManager = libSend.fileSenderManager(self.fileManager)

    def sendFromQueue(self):
        while 1:
            # print self.queue
            if(not self.queue):
                sleep(1)
                # print "not"
            else:
                fileData = self.queue[0]
                fileID = fileData[0]
                fileName = self.fileManager.fileNameFromID(fileID)
                fileSeq = fileData[1]
                address = fileData[2]
                filePriority = self.fileManager.getPriority(fileID)
                timeStamp = self.fileManager.getTimestamp(fileID)
                ttl = self.fileManager.getTTL(fileID)
                # print filePriority
                filePath = self.folder + "/" + fileName
                fileDest = self.fileManager.getDest(fileID)
                fileDestStatus = self.fileManager.getDestStatus(fileID)
                # print "SENDING"
                # print filePath
                # print self.fileSenderManager.onGoing
                if(len(self.fileSenderManager.onGoing) < MAX_SEND_ONGOING):
                    self.fileSenderManager.sendFile(
                        fileName, fileID, filePath, fileSeq, filePriority,
                        timeStamp, ttl, fileDest, fileDestStatus, address, self.communicator)
                del self.queue[0]

    def acceptAck(self, data, address):
        self.fileSenderManager.handleACK(data, address)

    def startSendFromQ(self):
        self.qThread = Thread(target=self.sendFromQueue, args=())
        self.qThread.setDaemon(True)
        self.qThread.start()


class folderReceiver(object):

    """folderReceiver"""

    def __init__(self, folderPath, communicator, fileManager):
        self.activeFiles = {}
        self.activeFileId = {}
        self.communicator = communicator
        self.fileManager = fileManager
        self.folderPath = folderPath

    def generateReceiveId(self, fileID, address):
        ip = str(address[0])
        port = str(address[1])
        hashString = (fileID + ip + port)
        receiveId = hashlib.sha1(hashString).hexdigest()
        return receiveId

    def receive(self, data, address):
        # print data[1]
        # chunkString = data[2]
        # chunkData = chunkString.decode('base64')
        fileName = data[1]
        # sequence = data[1]
        # filePriority = data[4]
        # print data
        fileID = data[0]
        fileSize = data[4]
        receiveId = self.generateReceiveId(fileID, address)

        if(receiveId in self.activeFiles):
            self.activeFiles[receiveId].receive(data, address)
        else:
            # print "created"
            # print receiveId
            self.activeFiles[receiveId] = fileReceiver(
                fileID, self.folderPath, receiveId, fileName, fileSize,
                self.activeFiles, self.activeFileId, self.communicator, self.fileManager)
            self.activeFiles[receiveId].startTimer()
            self.activeFiles[receiveId].receive(data, address)

        if(fileID in self.activeFileId):
            pass
        else:
            self.activeFileId[fileID] = address


class fileReceiver(object):

    """fileReceiver"""

    def __init__(self, fileId, folderPath, receiveId, fileName,
                 fileSize, activeFiles, activeFileId, communicator, fileManager):
        self.folderPath = str(folderPath)
        self.communicator = communicator
        self.fileManager = fileManager
        self.fileSize = fileSize
        self.receiveId = receiveId
        self.activeFiles = activeFiles
        self.activeFileId = activeFileId
        self.fileName = fileName
        self.timer = 0
        self.ended = False
        self.startTS = 0
        self.endTS = 0
        self.address = None
        self.fileId = fileId
        self.fileDest = "NONE"
        self.fileDestStatus = "NONE"

        self.f = 0
        self.LAS = -1
        self.LSN = -1
        self.store = {}

        self.localSeq = self.fileManager.fileStatus(self.fileId)
        if(self.localSeq):
            self.LOF = self.localSeq[1] - 1
        else:
            self.LOF = "NONE"

        self.lastRcvTimeStamp = time()
        self.startTimeStamp = time()
        self.firstSEQ = 0

    def receive(self, data, address):
        if(not self.address):
            self.address = address
        if(len(data) == 10):
            self.timeStamp = data[6]
            self.ttl = data[7]
            self.fileDest = data[8]
            self.fileDestStatus = data[9]
            if(self.LOF == "NONE"):
                if(self.fileDestStatus == -2):
                    self.LOF = -1
                else:
                    self.LOF = self.fileDestStatus
            elif(self.LOF<self.fileDestStatus):
                self.LOF = self.fileDestStatus
        if(len(data) >= 6):
            self.filePriority = data[5]
        sequence = data[2]
        self.fileId = data[0]
        fileName = data[1]
        chunkString = data[3]
        chunkData = chunkString
        fileSize = data[4]
        # print "seq:", sequence
        # print "lof", self.LOF

        # timer to 0-------------------------------
        self.timer = 0
        # RESET -----------------------------------
        if(sequence == "FINISH"):
            # print "RESET"
            try:
                self.f.close()
            except:
                pass
                # print "\n RESET FAILED \n"
            if(self.ended is False):
                # print self.ended
                self.fileManager.setSeqTo(self.fileId, -1)
                print "-----------------------------------------"
                print "FINISHED DOWNLOADING: " + fileName
                print "-----------------------------------------"
                self.stopTS = time()
                timeTaken = (self.lastRcvTimeStamp - self.startTimeStamp)
                sizeRec = (self.LOF - self.firstSEQ) * LENGTH
                if(timeTaken != 0):
                    speed = (sizeRec / 1024) / timeTaken
                else:
                    speed = 0
                print "Speed: " + str(speed) + " KBps"
                print "Time taken: " + str(self.stopTS - self.startTS)
                logList = [str(time()), "RECEIVED", str(fileName), str(
                    self.fileSize / 1024), str(address), str(speed)]
                # logTxt = "RECEIVED: " + str(fileName) + " " +
                # str(self.fileSize / 1024) + " KB " + str(speed) + "KBps" + "
                # from: " + str(address)
                logger.log(str(logList))
                try:
                    self.fileManager.checkChecksum(self.fileId)
                    self.fileManager.checkDest(self.fileId, -1, "ThisNode")
                except:
                    raise
            self.ended = True
            self.acknowledge(
                self.fileId, self.fileName, sequence, 'y', address)

        elif(sequence == "STOP"):
            # print "RESET"
            try:
                self.f.close()
            except:
                pass
                # print "\n RESET FAILED \n"
            if(self.ended is False):
                # print self.ended
                #self.fileManager.setSEQ(self.fileId, -1)
                print "-----------------------------------------"
                print "FINISHED DOWNLOADING PART: " + fileName
                print "-----------------------------------------"
                self.stopTS = time()
                timeTaken = (self.lastRcvTimeStamp - self.startTimeStamp)
                sizeRec = (self.LOF - self.firstSEQ) * LENGTH
                if(timeTaken != 0):
                    speed = (sizeRec / 1024) / timeTaken
                else:
                    speed = 0
                print "Speed: " + str(speed) + " KBps"
                print "Time taken: " + str(self.stopTS - self.startTS)
                logList = [str(time()), "RECEIVED PART", str(fileName), str(
                    self.fileSize / 1024), str(address), str(speed)]
                # logTxt = "RECEIVED: " + str(fileName) + " " +
                # str(self.fileSize / 1024) + " KB " + str(speed) + "KBps" + "
                # from: " + str(address)
                logger.log(str(logList))
                self.fileManager.checkDest(self.fileId, self.LOF, "ThisNode")
            self.ended = True
            self.acknowledge(
                self.fileId, self.fileName, sequence, 'y', address)

        # TERMINATION -----------------------------
        elif(chunkData == "END"):
            # f.close()
            self.acknowledge(self.fileId, fileName, sequence, 'END', address)
            # print "DONE!!, Waiting for RESET"

        else:
            # INITIATION --------------------------------------
            if(self.LOF != "NONE"):
                if(sequence == (self.LOF + 1) and (not self.f)):
                    self.f = open(self.folderPath + '/' + fileName, 'a')
                    sizeInKB = float(self.fileSize) / 1024.0
                    print "\n--------------  RECEIVING  ------------------------\n"
                    print "\n Receiving: " + fileName
                    print "\n Size: " + str(sizeInKB) + "KB"
                    print "\t From Seq: " + str(sequence)
                    self.startTimeStamp = time()
                    self.firstSEQ = sequence
                    if(sequence == 0):
                        logList = [str(time()), "RECEIVING", str(fileName),
                                   str(self.fileSize / 1024),
                                   str(address), str(sequence)]

                    #    logTxt = "RECEIVING: " + \
                    #        str(fileName) + " " + str(self.fileSize / 1024) + \
                    #        " KB " + " from: " + \
                    #        str(address) + " seq: " + str(sequence)

                    else:
                        logList = [str(time()), "RESUME-RECEIVING",
                                   str(fileName),
                                   str(self.fileSize / 1024),
                                   str(address), str(sequence)]

                    #    logTxt = "RESUME RECEIVING: " + \
                    #        str(fileName) + " " + str(self.fileSize / 1024) + \
                    #        " KB " + " from: " + \
                    #        str(address) + " seq: " + str(sequence)
                    logger.log(str(logList))
                    self.startTS = time()
                    if(not self.fileManager.fileStatus(self.fileId)):
                        entry = [self.fileName, [sequence, sequence],
                                 fileSize, self.filePriority,
                                 self.timeStamp, self.ttl, self.fileDest, self.fileDestStatus]
                        self.fileManager.updateEntry(self.fileId, entry)
                    else:
                        localSeq = self.fileManager.fileStatus(self.fileId)
                        localSeqFrom = localSeq[0]
                        localSeqTo = localSeq[1]
                        if(sequence==localSeqTo+1):
                            self.fileManager.setSeqTo(self.fileId, sequence)
                        else:
                            print "FATAL, FILE BREAK"


                # PROPAGATION --------------------------------------
                if(sequence == (self.LOF + 1)):
                    seekPosition = LENGTH * sequence
                    self.f.seek(seekPosition)
                    self.f.write(chunkData)
                    # print "boom"
                    self.fileManager.setSeqTo(self.fileId, sequence)
                    self.updateProgress(self.fileSize, self.f)
                    # print "written" + str(sequence)
                    self.acknowledge(self.fileId, fileName, sequence, 'y', address)
                    self.LOF += 1
                    self.LOF = self.checkStore(
                        self.store, self.LOF, self.f, self.fileSize)

                elif(sequence > (self.LOF + 1) and (self.f != 0)):
                    for i in range(sequence - (self.LSN + 1)):
                        self.acknowledge(self.fileId, fileName, (self.LSN + i + 1),
                                         'n', address)
                    self.store[sequence] = chunkData
                    self.acknowledge(self.fileId, fileName, sequence, 'y', address)
                elif(sequence <= self.LOF):
                    self.acknowledge(self.fileId, fileName, sequence, 'y', address)
                else:
                    pass
                    #self.acknowledge(self.fileId, fileName, sequence, 'y', address)

                self.LSN = sequence
                self.lastRcvTimeStamp = time()

    def acknowledge(self, fileID, fileName, sequence, status, addr):
        target = "fileTransporter"
        command = "acceptAck"
        ackData = [fileID, fileName, [(sequence, status)]]
        message = [target, command, ackData]
        self.communicator.sendAck(message, addr)
        # print "ACK: " + str(sequence) + status
        if(status == 'y' or status == 'END'):
            self.LAS = sequence

    # check store ----------------------------------
    def checkStore(self, Store, Last, File, fileSize):
        while 1:
            sequence = Last + 1
            if(sequence in Store):
                seekPosition = LENGTH * sequence
                File.seek(seekPosition)
                File.write(Store[Last + 1])
                # print Store[Last + 1]
                self.fileManager.setSeqTo(self.fileId, sequence)
                self.updateProgress(self.fileSize, File)
                # print "written" + str(sequence)
                Last += 1
            else:
                return Last
                break

    def updateProgress(self, fileSize, fileObj):
        fileObj.seek(0, os.SEEK_END)
        receivedSize = fileObj.tell()
        fileObj.seek(0)
        fullSize = fileSize
        percentage = float(receivedSize) / float(fullSize) * 100.0
        # sys.stdout.write("\r%f%%" % percentage)
        # sys.stdout.flush()
        self.progress = percentage

    def timeOuter(self):
        # print self.receiveId
        # print "LAUNCHED"
        while 1:
            # print self.timer
            sleep(1)
            self.timer += 1
            if(self.timer >= 10):
                break
        try:
            del self.activeFiles[self.receiveId]
            del self.activeFileId[self.fileId]
        except:
            pass
        else:
            pass
        if(self.ended is False):
            #print self.lastRcvTimeStamp
            #print self.startTimeStamp
            timeTaken = (self.lastRcvTimeStamp - self.startTimeStamp)
            sizeRec = (self.LOF - self.firstSEQ) * LENGTH

            if(timeTaken != 0):
                speed = (sizeRec / 1024) / timeTaken
            else:
                speed = 0
            print "TIMED OUT: " + str(self.fileName)
            self.fileManager.checkDest(self.fileId, self.LOF, "ThisNode")

            logList = [str(time()), "TIMED-OUT", str(self.fileName),
                       str(self.address),
                       str(self.LOF), str(speed)]

            # logTxt = "TIMED OUT: " + \
            #    str(self.fileName) + " from: " + str(self.address) + \
            #    "LastSeq: " + str(self.LOF) + "speed: " + str(speed)

            logger.log(str(logList))
            self.stopTS = time()
            try:
                self.fileManager.setSeqTo(self.fileId, (self.LOF + 1))
                # print "SUCCESSSS!!!!!!!!!!!!!"
                # print self.LOF
            except:
                pass
            else:
                pass

    def startTimer(self):
        self.timerThread = Thread(target=self.timeOuter, args=())
        self.timerThread.setDaemon(True)
        self.timerThread.start()
