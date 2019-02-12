#
# Author: Sean McNamara (tank@thursday.org) 
#   Date: 2018-12-01
#
# Note: Based on initial question/code posted by Ireos.
#

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
# Removing ContractSamples because I don't have the TestBed module loaded.
from ibapi.common import *
from ibapi.contract import *
import datetime
import threading
import time

RUN_FLAG = False;

class TestClient(EClient):
    # Problem 1:
    #   You are trying to make TestApp both an EClient and EWapper, but you don't initialize it as a
    #   EWrapper.  You also have created a different class for your EWrapper.  One possible fix is
    #   to move those functions into a separate TestWrapper class (which is what I've done here.)
    #
    #  This also means we move the error() methods to the listener.
    #
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
 
 
# Better to do this when you construct the query
#
#queryTime = (datetime.datetime.today() - datetime.timedelta(days=180)).strftime("%Y%m%d %H:%M:%S")

# Problem 2:
#  TestApp is your client, so this piece is not useful.
#
#client = EClient(EWrapper)

# Problem 3:
#  You are calling this method inline in the script, thus sequentially.  That means it will be called before
#  reaching your main() definition and before the "if __name__" conditional.
#
# This is wrong.  It should be moved INSIDE your main (or elsewhere.)
#
#client.reqHistoricalData(4101, ContractSamples.ContractSamples.USStockAtSmart(), queryTime, "1 M", "1 day", "MIDPOINT", 1, 1,False,[])

class TestWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)

    def error(self, reqId: TickerId, errorcode: int, errorString: str):
        print("Error= ", reqId, " ", errorcode, " ", errorString)
 
    def contratDetails(self, reqId: int, contractDetails: ContractDetails):
        print("ContractDetails: ", reqId, " ", contractDetails)
 
    def historicalData(self, reqId: int, bar: BarData):
        print("HistoricalData. ", reqId, " Date:", bar.date, "Open:", bar.open,
              "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume,
              "Count:", bar.barCount, "WAP:", bar.average)
 
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        global RUN_FLAG
        print("HistoricalDataEnd ", reqId, "from", start, "to", end)
        RUN_FLAG = False
        
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        print("HistoricalDataUpdate. ", reqId, " Date:", bar.date, "Open:", bar.open,
              "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume,
              "Count:", bar.barCount, "WAP:", bar.average)

    # (Potential) Problem 4:
    #   You need to wait for the connectAck to ensure the TWS handshake has completed.
    def connectAck(self):
        global RUN_FLAG
        print("Connect ACK")
        RUN_FLAG = True


    def nextValidId(self, orderId:int):
        self.nextOrderId = orderId
        print("I have nextValidId", orderId)

        
def main():
    global RUN_FLAG
    wrapper = TestWrapper()
    client = TestClient(wrapper)
    client.connect("127.0.0.1", 7496, 100)
    print("Done with connect()")

    # Problem 5: **I think this is the biggest "GOTCHA" in the API docs.
    #  When app.run() is called, the reader is launched, and this will block
    #  the main thread, meaning you'll never get to any of the stuff below.
    #  Instead, maybe launch it in a new thread...
    #
    # The thread management here is very rudimentary but should give you the idea.
    
    t = threading.Thread(name="TWSAPI_worker", target=client.run)
    t.start()
    print("Returned from run()")

    while not RUN_FLAG:
        time.sleep(1)
        print("Sleeping...")
    
    # Problem 5:
    #   I don't really know what you are trying to do here....
    # TestWrapper.historicalData(4101, BarData)
    
    # Create the contract
    contract = Contract()
    contract.symbol = "SPX"
    contract.secType = "NDX"
    contract.exchange = "SMART"
    contract.currency = "USD"

    queryTime = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y%m%d %H:%M:%S")

    client.reqHistoricalData(4101, contract, queryTime, "1 M", "1 day", "MIDPOINT", 1, 1,False,[])
    print("Returned from call to reqHistoricalData()")

    # NOTE:
    #  Because TWSAPI is async, you can't exit until all your responses are received
    #  (or you somehow timeout.)  There are many ways to address this, but you need to
    #  do *something* so you don't fall out of your run before all the reponses have
    #  been received/processed.
    print("Waiting to finish.")
    while RUN_FLAG:
        time.sleep(1)
        print("Sleeping...")

    client.disconnect()
    print("ALL DONE!")
        
if __name__ == "__main__":
    main()

#%%

from ib_insync import *
util.startLoop()

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=12)