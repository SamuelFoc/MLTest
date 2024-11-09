from MLTest.core.Sequences import Sequence
from pipes.preprocessing import _MergeData, _HandleDateColumns_, _ReplaceStrInColumns_, CastFillAndExport_


def MyPreprocessingSequence(sequence_args):
    sequence = Sequence(
        name="MySequence",
        pipelines=[
            _MergeData,
            _HandleDateColumns_,
            _ReplaceStrInColumns_,
            CastFillAndExport_
        ],
        args=sequence_args,
        log=True
    )
    return sequence