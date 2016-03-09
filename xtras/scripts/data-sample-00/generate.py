#!/usr/bin/python

import random

TOTAL_NRO_LINES=500000

def write(name, nroLines): 
    f = open("input/" + name + ".csv", "w" )
    for i in range(nroLines):
        line = str(random.randint(1, 700000)) + "," + str(random.randint(1,5000000)) 
        f.write(line + '\n')
    f.close()

print('*** Ensure the directory \'input\' exists...')

write('big1', 45000000)
write('big2', 20000000)
write('small1', 50000)
write('small2', 20000)
write('small3', 30000)
write('small4', 30000)
write('small5', 30000)

print('*** Done')


