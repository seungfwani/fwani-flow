from folder1 import bbb, ccc
from folder1.folder3 import ddd
from folder2 import eee

def printer():
    print("this is aaa printer")
    print("======== print others: ========")
    bbb.printer()
    ccc.printer()
    ddd.printer()
    eee.printer()
    return "ok"
