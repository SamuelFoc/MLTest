from MLTest.components.piping.Loaders import PipeLoader


loader = PipeLoader("pipes")

MergeDataPipe = loader.load_pipeline("MergeDataPipe")

merged_data = MergeDataPipe.run()
