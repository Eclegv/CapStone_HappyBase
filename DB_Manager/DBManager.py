from happybase import Connection
import time
import hashlib


class DBManager:
    def __init__(self, address, port, predicatesCatalog):
        self.address = address
        self.port = port
        self.predicatesCatalog = predicatesCatalog
        self.connection = Connection(self.address, port=self.port)
        self.tables = self.__getHexaIndexes()

    def deleteTables(self):
        self.connection.delete_table("SPOC", True)
        self.connection.delete_table("POSC", True)
        self.connection.delete_table("OSPC", True)
        self.connection.delete_table("CSPO", True)
        self.connection.delete_table("CPOS", True)
        self.connection.delete_table("COSP", True)

    def insertData(self, filename):
        f = open(filename, "r")
        lines = f.readlines()

        start_time = time.time()
        i = 0
        for line in lines:
            splitted = line.split(" ")
            if splitted[-1] == ".\n" and len(splitted) == 5:
                S = hashlib.md5(splitted[0].encode('utf_8')).hexdigest()
                P = None
                if splitted[1] in self.predicatesCatalog.catalog.keys():
                    P = str(self.predicatesCatalog.catalog[splitted[1]])
                else:
                    P = hashlib.md5(splitted[1].encode('utf_8')).hexdigest()
                O = hashlib.md5(splitted[2].encode('utf_8')).hexdigest()
                C = hashlib.md5(splitted[3].encode('utf_8')).hexdigest()

                keys = {
                    "SPOC": S + P + O + C,
                    "POSC": P + O + S + C,
                    "OSPC": O + S + P + C,
                    "CSPO": C + S + P + O,
                    "CPOS": C + P + O + S,
                    "COSP": C + O + S + P
                }

                for index in self.tables:
                    self.tables[index].put(keys[index].encode('utf-8'),
                                           {
                                               f"{index}:S".encode('utf_8'): splitted[0].encode('utf_8'),
                                               f"{index}:P".encode('utf_8'): splitted[1].encode('utf_8'),
                                               f"{index}:O".encode('utf_8'): splitted[2].encode('utf_8'),
                                               f"{index}:C".encode('utf_8'): splitted[3].encode('utf_8')
                                           })

                i += 1
                if i % 1000 == 0:
                    print(i)

        f.close()
        return (i, (time.time() - start_time))

    def requestDB(self, S="", P="", O="", C="", maxNumberResults=2000, rowKeyStart=None):
        filters = None
        scannedData = None
        tableName = self.__getTable(S, P, O, C)
        rowPrefix = self.__getRowPrefix(S, P, O, C, tableName)
        table = self.connection.table(tableName)
        results = []

        if (rowKeyStart != None):
            scannedData = table.scan(row_start=rowKeyStart)
        else:
            scannedData = table.scan(row_prefix=rowPrefix)

        cptRes = 0

        for key, value in scannedData:
            if (cptRes >= maxNumberResults or not self.__isPrefixMatching(key, rowPrefix)):
                rowKeyStart = key
                break
            results.append(value)
            cptRes += 1

        print(f"Nbr results : {cptRes}")
        print(f"Table used : {tableName}")
        print(f"Row prefix : {rowPrefix}")
        return {"Starting_RowKey": rowKeyStart, "Results_Count": cptRes, "Results": results}

    def __getTable(self, S, P, O, C):
        entries = {"S": S, "P": P, "O": O, "C": C}
        tablesNames = list(self.tables.keys())
        bestTableIndex = 0
        prefixLength = 0
        for i in range(len(tablesNames)):
            j = 0
            perm = tablesNames[i]
            for j in range(len(perm)):
                if entries[perm[j]] == "":
                    break
            if j > prefixLength:
                bestTableIndex = i
                prefixLength = j

        return tablesNames[bestTableIndex]

    def __getRowPrefix(self, S, P, O, C, tableName):
        entries = {"S": S, "P": P, "O": O, "C": C}
        prefix = None

        if S == "" and P == "" and O == "" and C == "":
            return prefix
        else:
            prefix = ""

        for letter in tableName:
            if entries[letter] == "":
                break
            if letter == "P" and entries["P"] in self.predicatesCatalog.catalog.keys():
                prefix += str(self.predicatesCatalog.catalog[entries[letter]])
            else:
                prefix += hashlib.md5(entries[letter].encode('utf_8')).hexdigest()

        return prefix.encode('utf_8')

    def __isPrefixMatching(self, rowKey, rowPrefix):
        if rowPrefix is None:
            return True
        return rowPrefix == rowKey[0:len(rowPrefix)]

    def __getHexaIndexes(self):
        tables = {
            "SPOC": self.connection.table('SPOC'),
            "POSC": self.connection.table('POSC'),
            "OSPC": self.connection.table('OSPC'),
            "CSPO": self.connection.table('CSPO'),
            "CPOS": self.connection.table('CPOS'),
            "COSP": self.connection.table('COSP')
        }
        return tables