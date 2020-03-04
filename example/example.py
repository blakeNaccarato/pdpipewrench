import custom_functions
import pdpipewrench as pdpw

src = pdpw.Source("example_source")
snk = pdpw.Sink("example_sink")
line = pdpw.Line("example_pipeline", custom_functions)
pipeline = line.connect(src, snk)
print(pipeline)
(dfs_in, dfs_out) = line.run()
