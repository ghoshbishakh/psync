#! /usr/bin/env python
def log(text):
    logtext = str(text)
    f = open('log.txt', 'a')
    f.seek(0, 2)
    f.write('\n')
    f.write(str(logtext))
    f.close()
    #print "\n"
    #print text
    #print "\n"
