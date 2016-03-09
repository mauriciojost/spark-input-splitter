#!/usr/bin/python

import random

TOTAL_NRO_LINES=500000

def write(name, nroLines): 
    f = open("data/" + name + ".csv", "w" )
    for i in range(nroLines):
        line = str(random.randint(1, 100000)) + "," + str(random.randint(1,100000)) 
        f.write(line)
    f.close()

print('*** Ensure data path is created...')

write('big', 5000000)
write('small', 10000)

print('*** Done')


