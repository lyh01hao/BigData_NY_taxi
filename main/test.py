from pyecharts.charts import Line
from pyecharts import options as opts

with open("./file/tripDistancePartition.txt") as f:
    tripDistancePartition = f.read()
    tripDistancePartition = tripDistancePartition.split('\n')
    f.close()

tripDistancePartition.pop(0)
tripDistancePartition.pop(-1)

tripDistancePartitionX = []
tripDistancePartitionY = []
for i in tripDistancePartition:
    i_s = i.split(' ')
    print(i_s[0])
    tripDistancePartitionX.append(i_s[0].split('-')[0])
    print(i_s[1])
    tripDistancePartitionY.append(i.split(' ')[1])

tripDistancePartitionLine = (Line()
        .add_xaxis(tripDistancePartitionX)
        .add_yaxis('', tripDistancePartitionY, is_symbol_show=False)
        .set_global_opts(title_opts=opts.TitleOpts(title="乘客乘车距离分布 单位为KM"))
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
          )

tripDistancePartitionLine.render("./visualization/tripDistancePartition.html")
