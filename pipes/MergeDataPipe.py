from MLTest.components.piping.Pipes import FlowPipe
from MLTest.components.storing.Storages import A_Storage
from MLTest.components.filesystem.Loads import ReadData
from MLTest.components.preprocessing.Merges import MergeStorage
from MLTest.components.filesystem.Exports import ExportData


PIPELINE = FlowPipe([
    A_Storage([
        ReadData("./data/sample_data.csv"),
        ReadData("./data/sample_data.json")
    ]),
    MergeStorage(how="join-inner", on="id"),
    ExportData(save_to="./merged_data.csv")
])