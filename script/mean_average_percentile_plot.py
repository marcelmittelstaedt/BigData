import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})
df = pd.read_csv("mean_average_percentile_data.csv")
print df
#df.set_index('time', inplace=True)
styles=['-','--','-']
linewidths = [3, 3, 3]
plot=df.plot(alpha=0.5,style=styles)
for i, l in enumerate(plot.lines):
    plt.setp(l, linewidth=linewidths[i])
plot.set_xlabel("time in minutes")
plot.set_ylabel("response time in ms")
plot.set_xticks(df.index[::2])
plot.set_xticklabels(df.time, rotation=45)
plot.grid(True)
fig = plot.get_figure()
fig.savefig("mean_average_percentile_plot.png")
