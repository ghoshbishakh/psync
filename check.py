import os
import hashlib

f = open('checksums.txt', 'a')
f.write('\n\n===================================================\n\n')
path = "sync"

for dirpath, dirnames, filenames in os.walk(path):
    for item in filenames:
        path = os.path.join(dirpath, item)
        FileId = hashlib.md5(open(path, 'rb').read()).hexdigest()
        f.seek(0, 2)
        f.write('\n')
        f.write(str(item) + "," + str(FileId))
f.close()