#! python3.x

from persistentdict import PersistentDict

pd = PersistentDict("dict.json")
pd.startTransaction()
pd.set()
pd.set()
pd.closeTransaction()
