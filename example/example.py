import custom_functions
import pdpipewrench as pdpw
from IPython.display import display_html

src = pdpw.Source("example_source")  # generate the source from `config.yaml`
snk = pdpw.Sink("example_sink")  # generate the sink from `config.yaml`.

# generate the pipeline from `config.yaml`.
line = pdpw.Line("example_pipeline", custom_functions)

# connect the source and sink to the pipeline, print what the pipeline will do, then run
# the pipeline, writing the output to disk. capture the input/output dataframes if
# desired.
dfs_in = line.connect(src, snk)
print(line.pipeline)
dfs_out = line.run()
for df_in, df_out in zip(dfs_in, dfs_out):
    html = df_in.to_html() + "  --->  " + df_out.to_html()
    html = html.replace("table", "table style='display:inline'")
    display_html(html, raw=True)
