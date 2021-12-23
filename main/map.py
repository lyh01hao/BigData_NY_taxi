import gmplot

apikey = 'AIzaSyDjf4ELvJ8qByhN27MLIf7R-Yevduw9ZNo'
gmap = gmplot.GoogleMapPlotter(40.72680379495213, -73.99566961317387, 14, apikey=apikey)
with open("./file/pickUpLocation.txt") as f:
    pickUpLocation = f.read()
    pickUpLocation = pickUpLocation.split('\n')
    f.close()

pickUpLocation.pop(0)
pickUpLocation.pop(-1)
print(pickUpLocation)

centers_lats, centers_lngs = zip(*[(float(i.split(' ')[1]), float(i.split(' ')[0])) for i in pickUpLocation])
gmap.scatter(centers_lats, centers_lngs, color='#ff0000', size=100, marker=True)
gmap.draw('./visualization/clusteringCenters.html')
