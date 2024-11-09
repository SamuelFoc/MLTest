from sequences.MyPreprocessingSequence import MyPreprocessingSequence
from MLTest.core.LoadArgs import load_args

sequence = MyPreprocessingSequence(load_args("flow_1.conf.py", "seq_1_args"))
sequence.run()