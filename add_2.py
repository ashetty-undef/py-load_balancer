from client import *
class Add(Client):
    def run(self,*args,**kwargs):
        print(args,kwargs)


def run(name):
    c = Add(name)
    c.mainloop()

if __name__ == "__main__":
    multiproc(run,"Check",3)


