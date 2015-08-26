import communicator
import discoverer
import fileManager
import controller
import fileTransporter
from time import sleep
import getIP
import logger
import time
import grouper


logList = [str(time.time()), "START"]
logger.log(str(logList))

PEER_IP = "192.168.10.209"
PEER_PORT = 55555
PEER_ID = 'D209'
FOLDER_PATH = 'sync'
print PEER_IP


def controlRouter(target, command, message, address):
    # print target, command, message, address
    if(target == 'controller'):
        getattr(Controller, command)(message, address)
    if(target == 'fileTransporter'):
        getattr(FileTransporter, command)(message, address)
    if(target == 'folderReceiver'):
        getattr(FolderReceiver, command)(message, address)
    if(target == 'fileSenderManager'):
        getattr(FolderReceiver.fileSenderManager, command)(message)
    if(target == 'grouper'):
        getattr(Grouper, command)(message, address)



def discoveryHandler(peerId, peerData, state):
    print peerId, peerData, state

    if(state == 1):
        status = "DISCOVERED-NODE"
    else:
        status = "LOST-NODE"

    logList = [str(time.time()), str(status), str(peerId), str(peerData)]
    logger.log(str(logList))


# -------------------------------------------------------------------------
Communicator = communicator.communicator(controlRouter, PEER_IP, PEER_PORT)


# -------------------------------------------------------------------------
Discoverer = discoverer.discoverer(
    PEER_IP, PEER_PORT, PEER_ID, discoveryHandler, '192.168.10.255')

#-------------------------------------------------------------------------

Grouper = grouper.grouper(Discoverer, Communicator, controlRouter, 3, 60)


# --------------------------------------------------------------------------
FileManager = fileManager.fileManager(FOLDER_PATH, Discoverer)

#---------------------------------------------------------------------------
FolderReceiver = fileTransporter.folderReceiver(
    FOLDER_PATH, Communicator, FileManager)

# ---------------------------------------------------------------------------
Controller = controller.controller(Communicator, Discoverer, Grouper, FileManager, FolderReceiver)

# --------------------------------------------------------------------------
FileTransporter = fileTransporter.fileTransporter(
    Controller, FOLDER_PATH, Communicator, FileManager)


Communicator.run()
Discoverer.startDiscovery()
Grouper.start()
FileManager.start()
FileTransporter.startSendFromQ()
sleep(5)
print Discoverer.dbPresent()
while 1:
    a = raw_input()
    if(a == 'remote'):
        Controller.updateRemoteFiles()
        print Controller.allRemoteFiles
    if(a == 'missing'):
        Controller.updateMissingFiles()
        print Controller.missingFiles
    if(a == 'sync'):
        Controller.startSync()
    if(a == 'show'):
        FileManager.showFiles()
    if(a == 'autosync'):
        logList = [str(time.time()), "AUTOSYNC"]
        logger.log(str(logList))
        while 1:
		    Controller.startSync()
		    sleep(5)
            #print "syncing"
    if(a == "update"):
        FileManager.updateFromFolder()
    if(a == 'active'):
        print FolderReceiver.activeFiles
    if(a == 'setttl'):
        b = raw_input('fileID:')  # fileid
        c = raw_input('ttl: ')  # ttl
        FileManager.setTTL(b, c)

logList = [str(time.time()), "INTERRUPT"]
logger.log(str(logList))
