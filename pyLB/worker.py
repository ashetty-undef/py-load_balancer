from supervisor import *




# name = "test"
# num_process = 10
# sup = Supervisor(name,num_process)
mgmr = Initiator()
# sup = mgmr.register(name,num_process)
mgmr.manager.connect()
sup = mgmr.manager.Supervisor()

sup.push(30)
sup.display()
