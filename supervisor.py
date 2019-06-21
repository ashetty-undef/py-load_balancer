from threading import Timer

from redis import StrictRedis
import logging
from collections import deque
import math
from multiprocessing.managers import BaseManager
import pickle
import sys
import signal

class MyManager(BaseManager):
    pass


class RepeatedTimer(object):
    """
    The following code creates thread for performing a function in a specific interval of time

    """
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

def fair_distribution(inp, out):
    '''
    fair scheduler -> gets the percentage of distribution across all the engines
    and then distributes it accordingly from the inp_queue


    ---------------------------------
    args

    out_queue -> list of the size of the output queues
    inp_queue -> size of the input queue

    -----------------------------------
    TODO: Make inp_queue be a list so that we can take input from mulitple partitions and distribute accordingly
    '''
    out_total = sum(out.values())
    inp_t = inp
    out_t = sorted(out,key=out.get,reverse=True)
    res = {}
    for i in out_t:
        cent_t = out[i]/out_total
        val = min(inp_t,math.ceil(inp * cent_t))
        res[i] = val
        inp_t = inp_t - val
    return res


class Supervisor:
    """

    Supervisor has access over the central queue and distributes the task over redis queues.

    """

    def __init__(self, name):
        """

        :param name: name of the Supervisor to be registered to the server

        """
        self.name = name
        self.queue = deque()
        self.redis_con = StrictRedis(db=1)
        self.con_names = set()
        self.prefetch = 1000
        self.thread = RepeatedTimer(10,self.schedule)
        self.thread.start()
        logging.basicConfig(level=logging.DEBUG)


    def add(self, name):
        """
        Add a new Supervisor connection to this server
        :param name:  Name of the connection
        :return: None
        """
        try:
            self.con_names.add(name)
            print("added {}".format(name))
        except Exception as ex:
            logging.debug("Supervisor.add threw exception {}".format(ex))

    def remove(self, name):
        """
        Remove the Supervisor connection from this server
        :param name: Remove the connection
        :return:
        """
        try:
            self.con_names.remove(name)
            print("removed {}".format(name))
        except Exception as ex:
            logging.debug("Supervisor.remove threw exception {}".format(ex))

    def distribute(self, distrib):
        """

        :param machines: list of machines who have registered to the service
        :param distrib: dictionary of current load for each machine
        :return:
        """
        for mach, count in distrib.items():
            a = []
            for _ in range(count):
                v = self.pop()
                if v is None:
                    break
                a.append(pickle.dumps(v))
            if len(a):
                self.redis_con.lpush(mach, *a)

    def schedule(self):
        machines = {}
        for machine in self.con_names:
            redis_size = max(32 - self.redis_con.llen(machine), 0)
            machines[machine] = redis_size

        if len(machines):
            distrib = fair_distribution(len(self.queue),machines)
            logging.info("schedule : new {}".format(distrib))
            self.distribute(distrib)

    def push(self, val):
        try:
            self.queue.append(val)
        except Exception as ex:
            logging.error("Push: Queue couldnt push {}".format(ex))
        self.display()
        self.schedule()

    def pop(self):
        return self.queue.pop()

    def display(self):
        print(self.con_names, self.queue, {i: self.redis_con.llen(i) for i in self.con_names})

    def tasks(self, func):
        '''

        :param func:
        :return: wrapper

        This wrapper checks for name argument to pop elements from the redis queue and feed it to the respective task
        '''

        def process(*args, **kwargs):
            if args:
                if hasattr(args[0], 'name'):
                    obj = args[0]
                    name = obj.name
                else:
                    raise Exception("Object has no name attr")
            elif kwargs.get("name", None):
                name = kwargs["name"]
                kwargs.pop("name")

            else:
                raise Exception("No object defined or name is given")

            self.schedule()
            self.display()
            kwargs["val"] = self.redis_con.lpop(name)
            logging.debug('tasks,args -{} and kwargs -{}'.format(args, kwargs))
            return func(*args, **kwargs)

        return process


class Initiator:
    '''

    starts the server for creating new instances for the processes
    '''

    def __init__(self, IP='0.0.0.0', PORT=8999, AUTHKEY=b'abcd'):
        self.manager = MyManager(address=(IP, PORT), authkey=AUTHKEY)
        self.registered_sup = {}

    def register(self, name):
        """

        :param name: used track the process and its respective queues

        """
        sup = Supervisor(name)

        MyManager.register(name, callable=lambda: sup)
        self.registered_sup[name] = sup
        logging.debug('registered {} into the Controller'.format(name))

    def get_supervisor(self, name):
        return self.registered_sup[name]

    def start(self):
        server = self.manager.get_server()
        server.serve_forever()

    def get_connection(self, name):
        self.manager.connect()
        print(self.manager.__getattribute__(name))
        return self.manager.__getattribute__(name)


class Signalhandler:

    def __init__(self, server):
        self.server = server

    def server_shutdown(self):
        self.server.shutdown()


name = "Supervisor"
server = Initiator()

# registering all processes before starting the server
server.register(name)

if __name__ == "__main__":
    print("Starting server")
    server.start()
