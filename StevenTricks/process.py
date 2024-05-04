
from time import sleep
from random import randint

def sleepteller(mode=None):
    if mode == 'long':
        time = randint(600, 660)
    else:
        time = randint(10, 30)
    print('Be about to sleep {}'.format(str(time)))
    sleep(time)