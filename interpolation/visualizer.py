"""

"""

from matplotlib import pyplot


def draw_map(lat_lon_label_rows):
    """
    draws prototypical map of the given information
    
    :param lat_lon_label_rows: [(53.64159, 9.94502, "test")]
    :return: 
    """
    fig, ax = pyplot.subplots()
    lats, lons, labels = zip(*lat_lon_label_rows)
    ax.scatter(lons, lats)
    for lat, lon, label in lat_lon_label_rows:
        ax.scatter(lon, lat)
    for lat, lon, label in lat_lon_label_rows:
        ax.annotate(label, (lon, lat))
    pyplot.show()


def demo():
    lat_lon_label_rows = [
        (53.64159, 9.94502, "center"),
        (53.84159, 9.94502, "north"),
        (53.44159, 9.94502, "south"),
        (53.64159, 9.74502, "west"),
        (53.64159, 10.14502, "east")
    ]
    draw_map(lat_lon_label_rows)

if __name__ == "__main__":
    demo()
