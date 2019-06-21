import redis
from supervisor import *
import logging
from multiprocessing import Pool,Event
import time
import signal

class Client:
    """
    This is the client class for connecting to the load balancer server
    """
    def __init__(self,name,event = None,*args,**kwargs):
        self.name = name
        self.event = event
        #setting up the redis connection
        self.redis_con = redis.StrictRedis(db=1)
        #setting up server connections
        self.server = Initiator()
        self.server.manager.connect()
        self.sup = self.server.manager.Supervisor()
        #register the client to the server
        self.sup.add(name)


    def run(self,*args,**kwargs):
            pass


    def mainloop(self):
        try:
            while True:
                if self.redis_con.llen(self.name):
                    try:
                        val = self.redis_con.lpop(self.name)
                        self.run(val=val)
                    except Exception as ex:
                        logging.debug("mainloop entered into exception {} ".format(ex))
                else:
                    time.sleep(1)
        except KeyboardInterrupt:
            self.__del__()


    def wrapper(self,func):
        """
        The follwing wrapper will pop the data from redis queue and feed a set of variables as kwargs

        val -> the value poppped from the redis

        :param func:
        :return:
        """
        def process(*args,**kwargs):
            if self.redis_con.llen(self.name):
                try:
                    val = self.redis_con.lpop(self.name)
                    self.run(name=val)
                except Exception as ex:
                    logging.info("process poppping encountered exception : {}".format(ex))
            else:
                time.sleep(0.2)
            return func(*args,**kwargs)
        return process

    def loop(self):
        """
        The following will run the code wrapped code in loop
        :return:
        """
        try:
            while True:
                if not self.event.isset():
                    try:
                        self.run()
                    except Exception as ex:
                        logging.debug("loop entered into an exception : {}".format(ex))
                else :
                    break
        except KeyboardInterrupt:
            self.__del__()

    def __del__(self):
        """
        Removing the entry from supervisor
        """
        logging.info("The client {} is exiting ".format(self.name))
        self.sup.remove(self.name)
        sys.exit(0)


class MultiProcess:
    """
    Spwans multiple process

    """
    def __init__(self,func,name,num_process):
        """

        :param func: function to be spawned
        :param name:
        :param num_process:
        """
        self.func = func
        self.name = name
        self.num_process = num_process
        self.Event = Event()

    def spawn(self):
        with Pool(self.num_process) as p:
            p.map(self.func, list((":".join((name, str(i))),self.Event) for i in range(self.num_process)))

    def signal_handler(self):
        self.Event.set()

def multiproc(func,name,num_process=2):

    with Pool(num_process) as p:
         p.map(func,list(":".join((name,str(i))) for i in range(num_process)))

