from MLTest.core.Sequences import SequenceLoader
from MLTest.core.Strategies import UseStrategy


def test_1():
    loader = SequenceLoader("sequences")

    clean_and_merge = loader.load_sequence("Clean&Merge")
    clean_data = clean_and_merge.run()

    strategic_data = UseStrategy("strategies").use("strategy_1", clean_data)

    return strategic_data
