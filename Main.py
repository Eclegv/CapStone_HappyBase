import time

import matplotlib.pyplot as plt
from Catalog.predicatesCataloger import predicatesCataloger
from DB_Manager.DBManager import DBManager

filenames = ["Predicates/schemaorgPredicates.csv"]

predicatesCatalog = predicatesCataloger(filenames)
DB = DBManager("localhost", 9090, predicatesCatalog)

print(f"Table : {list(DB.tables.keys())}")

# # Benchmarking the querying

previousKey = None
startKey = None
totalTime = 0
nbrReq = 0
totalElements = 0
requestsTime = []
requestsNumbers = []

while True:
    print(f"\nRequest Number : {nbrReq + 1}, Starting key : {startKey}")

    start_time = time.time()

    outPutReq = DB.requestDB(S="", P="", O="", C="", maxNumberResults=10, rowKeyStart=startKey)
    startKey = outPutReq["Starting_RowKey"]
    totalElements += outPutReq["Results_Count"]

    timing = (time.time() - start_time)

    print(f"--- {timing} seconds ---")

    nbrReq += 1
    totalTime += timing
    requestsNumbers.append(nbrReq)
    requestsTime.append(timing)

    if startKey == previousKey:
        break
    else:
        previousKey = startKey

print("---- Benchmark results ----")
print(f"Total time : {totalTime} seconds\n ")
print(f"Average time by request : {totalTime / nbrReq}\n")
print(f"Number of requests : {nbrReq}\n")
print(f"Number of elements : {totalElements}")

plt.plot(requestsNumbers, requestsTime)
plt.xlabel('Request Number')
plt.ylabel('Request Time')
plt.show()
