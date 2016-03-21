#!/usr/bin/python

import random
import subprocess as sub

TOTAL_NRO_LINES=500000

def write(name, nroLines):
    filename = "input/" + name + ".csv"
    f = open(filename, "w" )
    for i in range(nroLines):
        line = str(random.randint(1, 700000)) + "," + str(random.randint(1,5000000)) 
        f.write(line + '\n')
    f.close()

    p = sub.Popen(['gzip', filename], stdout=sub.PIPE, stderr=sub.PIPE)
    output, errors = p.communicate()
    print output


print('*** Ensure the directory \'input\' exists...')

write('small1', 50000)
write('small2', 20000)
write('small3', 30000)
write('small4', 30000)
write('small5', 30000)
write('big1', 45000000)
write('big2', 12000000)


print('*** Done')


