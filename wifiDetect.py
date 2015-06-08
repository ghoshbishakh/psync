import os

iwlistOutput = os.popen("iwlist wlan0 scan")
#print iwlistOutput.read()
for line in iwlistOutput.readlines():
    if(("Address:" in line) or ("ESSID:" in line) or ("Mode:" in line)):
        print line


