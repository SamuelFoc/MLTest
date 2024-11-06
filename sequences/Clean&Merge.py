from MLTest.core.Sequences import Sequence
from MLTest.core.Pipes import PipeLoader


loader = PipeLoader("pipes")

MergeDataPipe = loader.load_pipeline("MergeDataPipe")
CleanDataPipe = loader.load_pipeline("CleanDataPipe")

SEQUENCE = Sequence([
    MergeDataPipe,
    CleanDataPipe
])
