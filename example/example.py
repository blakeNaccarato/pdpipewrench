import custom_functions
import pdpipewrench as pdpw

src = pdpw.Source("example_source")  # generate the source from `config.yaml`
snk = pdpw.Sink("example_sink")  # generate the sink from `config.yaml`.

# generate the pipeline from `config.yaml`.
line = pdpw.Line("example_pipeline", custom_functions)

# connect the source and sink to the pipeline, print what the pipeline will do, then run
# the pipeline, writing the output to disk. capture the input/output dataframes if desired.
pipeline = line.connect(src, snk)
print(pipeline)
(dfs_in, dfs_out) = line.run()