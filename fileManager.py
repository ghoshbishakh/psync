# File Table and File List Format:
# ---------------------------------------------------------------------------------------------------------------------
# |  File ID  |  File Name  |  Sequence  |  File Size  | Priority | Timestamp | TTL | Destination | DestReachedStatus |
# ---------------------------------------------------------------------------------------------------------------------


import hashlib
import os
import json
from threading import Thread
import time
import logger
from random import randint


class fileManager(object):

    """tracks and manages local files

    Arguments:

    folderPath - name of the folder shared
    databaseName - name of database to be used (default: fileManager.db)
    """

    def __init__(self, folderPath, discoverer, dbFileName='fileDB.txt'):
        self.folderPath = folderPath
        self.dbFileName = dbFileName
        self.discoverer = discoverer
        self.tmpDB = {}
        try:
            self.fileDB = open(self.dbFileName, "r+")
        except:
            self.fileDB = open(self.dbFileName, "w")
            self.fileDB.write("{}")
            self.fileDB.close()
            self.fileDB = open(self.dbFileName, "r+")
        self.stopUpdating = True
        self.updateTmpFromFile()
        self.updated = False

    def updateTmpFromFile(self):
        self.fileDB.seek(0, 0)
        jsonData = self.fileDB.read()
        data = json.loads(jsonData)
        self.tmpDB = data

    def updateFileDB(self):
        self.fileDB.seek(0, 0)
        self.fileDB.truncate()
        self.fileDB.seek(0, 0)
        jsonData = json.dumps(self.tmpDB)
        self.fileDB.write(str(jsonData))
        self.fileDB.seek(0, 0)
        # print "updated"
        # print self.fileDB.read()

    def fileExistsByName(self, fileName):
        '''Check if file exists in folder

        Arguments:
        filename - name of file

        Returns: True/False
        '''
        path = self.folderPath
        exists = False
        for dirpath, dirnames, filenames in os.walk(path):
            for item in filenames:
                if(item == fileName):
                    exists = True
                    return exists
                else:
                    pass
        return exists

    def fileStatus(self, fileId):
        '''Check if file entry exists in table
        and returns Sequence

        Arguments:
        fileId

        Returns: False - if file does not exist
                 -1 - full file exists
                 seq - part of file exists
        '''
        if(fileId in self.tmpDB):
            return self.tmpDB[fileId][2]
        else:
            return False

    def fileNameFromID(self, fileID):
        if(fileID in self.tmpDB):
            fileName = self.tmpDB[fileID][1]
            return fileName
        else:
            return False

    def getPriority(self, fileID):
        if(fileID in self.tmpDB):
            filePriority = self.tmpDB[fileID][4]
            return filePriority
        else:
            return False

    def getTTL(self, fileID):
        if(fileID in self.tmpDB):
            fileData = self.tmpDB[fileID]
            # print fileData[5]
            return fileData[6]
        else:
            return False

    def getTimestamp(self, fileID):
        if(fileID in self.tmpDB):
            fileData = self.tmpDB[fileID]
            # print fileData[4]
            return fileData[5]
        else:
            return False

    def getSize(self, fileID):
        if(fileID in self.tmpDB):
            fileData = self.tmpDB[fileID]
            # print fileData[4]
            return fileData[3]
        else:
            return False

    def getDest(self, fileID):
        if(fileID in self.tmpDB):
            fileData = self.tmpDB[fileID]
            # print fileData[4]
            return fileData[7]
        else:
            return False

    def getDestStatus(self, fileID):
        if(fileID in self.tmpDB):
            fileData = self.tmpDB[fileID]
            # print fileData[4]
            return fileData[8]
        else:
            return False

    def setDestStatus(self, fileID, status):
        if(fileID in self.tmpDB):
            fileData = self.tmpDB[fileID]
            fileData[8] = status
            self.updated = True

    def getFiles(self):
        '''get list of files - returns a list form of fileTable'''
        List = []
        for fileId, Data in self.tmpDB.items():
            fileID = fileId
            fileName = Data[1]
            fileSequence = Data[2]
            fileSize = Data[3]
            filePriority = Data[4]
            timeStamp = Data[5]
            ttl = Data[6]
            dest = Data[7]
            destStatus = Data[8]
            fileInfo = [fileID, fileName, fileSequence, fileSize,
                        filePriority, timeStamp, ttl, dest, destStatus]
            List.append(fileInfo)
        # print List
        return List

    def setSEQ(self, fileID, seq):
        if(fileID in self.tmpDB):
            if(self.tmpDB[fileID][2] != -1):
                self.tmpDB[fileID][2] = seq
                #print seq
                self.updated = True

    def insertData(self, fileID, data):
        if(fileID in self.tmpDB):
            pass
        else:
            self.tmpDB[fileID] = data
            self.updated = True

    def updateEntry(self, fileId, fileData):
        #print fileData
        #print fileId
        entry = [fileId] + fileData
        #print entry
        self.tmpDB[fileId] = entry
        self.updated = True

    def checkDest(self, fileID, address):
        if(address == "ThisNode"):
            peerID = self.discoverer.peerID
        else:
            peerID = self.discoverer.getID(address)
        if(fileID in self.tmpDB):
            dest = self.tmpDB[fileID][7]
            if((dest != "ALL") and (dest == peerID)):
                self.tmpDB[fileID][8] = 1
            self.updated = True

    def checkChecksum(self, fileId):
        try:
            print "CHECKSUM CALLED"
            logList = [str(time.time()), "CHECKSUM FILE NOT FOUND IN ANY LIST", str(fileId)]
            folderPath = self.folderPath
            if(fileId in self.tmpDB):
                # print "in table"
                fileName = self.tmpDB[fileId][1]
                filePath = os.path.join(folderPath, fileName)
                calcContentId = hashlib.md5(open(filePath, 'rb').read()).hexdigest()
                calcNameId = hashlib.md5(str(fileName)).hexdigest()
                calcFileId = hashlib.md5(str(calcContentId)+str(calcNameId)).hexdigest()
                # print calcFileId
                # print fileId
                if(calcFileId == fileId):
                    logList = [str(time.time()), "CHECKSUM OK", str(fileName), str(fileId)]
                else:
                    os.remove(filePath)
                    del self.tmpDB[fileId]
                    logList = [str(time.time()), "CHECKSUM FAILED - FILE REMOVED", str(fileName), str(fileId)]
            logger.log(str(logList))
            print logList
        except:
            raise



    def updateFromFolder(self):
        # ADD NEW FILES ---------------------------------------------------
        # traverse the files in folder --------------
        path = self.folderPath
        for dirpath, dirnames, filenames in os.walk(path):
            for item in filenames:
                exists = False
                #print "ITEM", item
                for fileId, fileData in self.tmpDB.items():
                    #print fileData
                    if(item == fileData[1]):
                        exists = True
                    else:
                        pass
                if(not exists):
                    path = os.path.join(dirpath, item)
                    contentId = hashlib.md5(open(path, 'rb').read()).hexdigest()
                    nameId = hashlib.md5(str(item)).hexdigest()
                    FileId = hashlib.md5(str(contentId) + str(nameId)).hexdigest()
                    if(FileId in self.tmpDB):
                        pass
                    else:
                        fileEntry = [FileId, item, -1, os.path.getsize(path),
                                     randint(1,4), time.time(), -1, "All", 0]
                        self.tmpDB[FileId] = fileEntry
                        self.updated = True

                        logList = [
                            str(time.time()), "FILE_ADDED", str(item), str(FileId)]
                        logger.log(str(logList))
                        print logList

        # DELETE UNAVAILABLE FILES -------------------------------------------
        for fileId, data in self.tmpDB.items():
            if(self.fileExistsByName(data[1]) is True):
                pass
            elif(data[8] == 1):
                pass
            else:
                del self.tmpDB[fileId]
                self.updated = True

                logList = [
                    str(time.time()), "FILE REMOVED",
                    str(fileId), str(data[1])]
                logger.log(str(logList))
                print logList
        # print self.updated
        if(self.updated):
            self.updateFileDB()
            self.updated = False

    def startUpdating(self):
        while(not self.stopUpdating):
            self.updateFromFolder()
            time.sleep(5)

    def stop(self):
        self.stopUpdating = True

    def start(self):
        self.stopUpdating = False
        fileManagerThread = Thread(target=self.startUpdating, args=())
        fileManagerThread.setDaemon(True)
        fileManagerThread.start()

if __name__ == '__main__':
    manager = fileManager('sync')
    manager.run()
    x = raw_input()
