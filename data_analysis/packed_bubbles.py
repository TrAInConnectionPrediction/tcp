"""
===================
Packed-bubble chart
===================
Create a packed-bubble chart to represent scalar data.
The presented algorithm tries to move all bubbles as close to the center of
mass as possible while avoiding some collisions by moving around colliding
objects. In this example we plot the market share of different desktop
browsers.
(source: https://gs.statcounter.com/browser-market-share/desktop/worldwidev)
"""

import numpy as np
import matplotlib.pyplot as plt

# import os
# import sys
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# from helpers import profile

class BubbleChart:
    def __init__(self, area, bubble_spacing=0):
        """
        Setup for bubble collapse.
        Parameters
        ----------
        area : array-like
            Area of the bubbles.
        bubble_spacing : float, default: 0
            Minimal spacing between bubbles after collapsing.
        Notes
        -----
        If "area" is sorted, the results might look weird.
        """
        self.area = np.array(area, dtype=np.int64)
        self.radius = np.ceil(np.sqrt(self.area / np.pi)).astype(np.int64)

        self.bubble_spacing = bubble_spacing
        # self.bubbles[:, 2] = r
        # self.bubbles[:, 3] = area
        self.maxstep = 2 * self.radius.max() + self.bubble_spacing
        self.step_dist = self.maxstep / 2

        # calculate initial grid layout for bubbles
        length = np.ceil(np.sqrt(len(self.area)))
        grid = np.arange(length) * self.maxstep
        gx, gy = np.meshgrid(grid, grid)
        self.bubbles = np.ones((len(self.area), 2))
        self.bubbles[:, 0] = gx.flatten()[:len(self.bubbles)]
        self.bubbles[:, 1] = gy.flatten()[:len(self.bubbles)]

        self.com = self.center_of_mass()

    def center_of_mass(self):
        return np.average(
            self.bubbles, axis=0, weights=self.area
        )

    def center_distance(self, bubble, bubbles):
        return np.hypot(bubble[0] - bubbles[:, 0],
                        bubble[1] - bubbles[:, 1])

    def outline_distance(self, bubble, bubbles, radius, radii):
        center_distance = self.center_distance(bubble, bubbles)
        return center_distance  - radii - (self.bubble_spacing + radius)

    def check_collisions(self, bubble, bubbles, radius, radii):
        distance = self.outline_distance(bubble, bubbles, radius, radii)
        return (distance < 0).any()

    def collides_with(self, bubble, bubbles, radius, radii):
        distance = self.outline_distance(bubble, bubbles, radius, radii)
        if not (distance < 0).any():
            return []
        idx_min = np.argmin(distance)
        return idx_min if isinstance(idx_min, np.ndarray) else [idx_min]

    # @profile
    def collapse(self, n_iterations=50):
        """
        Move bubbles to the center of mass.
        Parameters
        ----------
        n_iterations : int, default: 50
            Number of moves to perform.
        """
        for _i in range(n_iterations):
            print(_i)
            self.com = self.center_of_mass()
            moves = 0
            for i in range(len(self.bubbles)):
                print(i, end='\r')
                rest_bub = np.delete(self.bubbles, i, 0)
                rest_radii = np.delete(self.radius, i, 0)
                # try to move directly towards the center of mass
                # direction vector from bubble to the center of mass
                dir_vec = self.com - self.bubbles[i, :]

                # shorten direction vector to have length of 1
                dir_vec = dir_vec / np.sqrt(dir_vec.dot(dir_vec))

                # calculate new bubble position
                new_bubble = self.bubbles[i, :] + dir_vec * self.step_dist
                # new_bubble = np.append(new_point, self.bubbles[i, 2:4])

                # # check whether new bubble collides with other bubbles
                # if not self.check_collisions(new_bubble, rest_bub, self.radius[i], rest_radii):
                #     self.bubbles[i, :] = new_bubble
                #     self.com = self.center_of_mass()
                #     moves += 1
                # else:
                # try to move around a bubble that you collide with
                # find colliding bubble
                collisions = self.collides_with(new_bubble, rest_bub, self.radius[i], rest_radii)
                if not collisions:
                    self.bubbles[i, :] = new_bubble
                    # self.com = self.center_of_mass()
                    moves += 1
                for colliding in collisions:
                    # calculate direction vector
                    dir_vec = rest_bub[colliding, :] - self.bubbles[i, :]
                    dir_vec = dir_vec / np.sqrt(dir_vec.dot(dir_vec))
                    # calculate orthogonal vector
                    orth = np.array([dir_vec[1], -dir_vec[0]])
                    # test which direction to go
                    new_point1 = (self.bubbles[i, :] + orth *
                                    self.step_dist)
                    new_point2 = (self.bubbles[i, :] - orth *
                                    self.step_dist)
                    dist1 = self.center_distance(
                        self.com, np.array([new_point1]))
                    dist2 = self.center_distance(
                        self.com, np.array([new_point2]))

                    new_bubble = new_point1 if dist1 < dist2 else new_point2
                    # new_bubble = np.append(new_point, self.bubbles[i, 2:4])
                    if not self.check_collisions(new_bubble, rest_bub, self.radius[i], rest_radii):
                        self.bubbles[i, :] = new_bubble
                        # self.com = self.center_of_mass()

            if moves / len(self.bubbles) < 0.1:
                self.step_dist = self.step_dist / 2

    def plot(self, ax, labels, colors):
        """
        Draw the bubble plot.
        Parameters
        ----------
        ax : matplotlib.axes.Axes
        labels : list
            Labels of the bubbles.
        colors : list
            Colors of the bubbles.
        """
        for i in range(len(self.bubbles)):
            circ = plt.Circle(
                self.bubbles[i, :2], self.radius[i], color=colors[i])
            ax.add_patch(circ)
            ax.text(*self.bubbles[i, :2], labels[i],
                    horizontalalignment='center', verticalalignment='center')


if __name__ == "__main__":
    browser_market_share = {
        'browsers': ['firefox', 'chrome', 'safari', 'edge', 'ie', 'opera'],
        'market_share': [8.61*1e2, 69.55*1e2, 8.36*1e2, 4.12*1e2, 2.76*1e2, 2.43*1e2],
        'color': ['#5A69AF', '#579E65', '#F9C784', '#FC944A', '#F24C00', '#00B825']
    }

    bubble_chart = BubbleChart(area=browser_market_share['market_share'],
                               bubble_spacing=0.1)

    bubble_chart.collapse()

    fig, ax = plt.subplots(subplot_kw=dict(aspect="equal"))
    bubble_chart.plot(
        ax, browser_market_share['browsers'], browser_market_share['color'])
    ax.axis("off")
    ax.relim()
    ax.autoscale_view()
    ax.set_title('Browser market share')

    plt.show()
