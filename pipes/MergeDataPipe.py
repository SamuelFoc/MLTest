from MLTest.core.Pipes import FlowPipe
from MLTest.components.storage.Input import StoreInputs
from MLTest.components.filesystem.Input import LoadData
from MLTest.components.preprocessing.Regulation import MergeStorage
from MLTest.components.filesystem.Export import ExportData


PIPELINE = FlowPipe([
    StoreInputs([
        LoadData("./data/cards.pq"),
        LoadData("./data/users.pq"),
        LoadData("./data/transactions.pq"),
    ]),
    MergeStorage(how="join-inner", on="User"),
    ExportData(save_to="./exports/merged_data.pq")
])