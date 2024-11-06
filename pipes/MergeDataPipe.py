from MLTest.core.Pipes import FlowPipe
from MLTest.components.storing.Storages import A_Storage
from MLTest.components.filesystem.Loads import ReadData
from MLTest.components.preprocessing.Merges import MergeStorage
from MLTest.components.filesystem.Exports import ExportData


PIPELINE = FlowPipe([
    A_Storage([
        ReadData("./data/cards.pq"),
        ReadData("./data/users.pq"),
        ReadData("./data/transactions.pq"),
    ]),
    MergeStorage(how="join-inner", on="User"),
    ExportData(save_to="./exports/merged_data.pq")
])