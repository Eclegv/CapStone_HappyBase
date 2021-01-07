class predicatesCataloger:
    def __init__(self, filenames):
        self.filenames = filenames
        self.catalog = self.__createCatalog(self.filenames)

    def __createCatalog(self, filenames):
        predicatesCatalog = {}
        ID = 1

        for filename in filenames:
            file = open(filename, "r")
            predicates = file.readlines()

            for predicate in predicates:
                predicatesCatalog["<" + predicate.rstrip() + ">"] = ID
                ID += 1

        return predicatesCatalog