import matplotlib.pyplot as plt
import numpy as np


# plt.ylabel('Write throughput (1000 PUTs/sec)')
# plt.xlabel('Number of users')
# plt.show()
#

plt.ylabel('Read throughput (thousand GETs/sec)')
x = [1000, 2000, 3000, 4000, 5000]
y_basic = [209991, 210930, 210834, 208636,
y_complex = [239838, 240664, 243755, 241989, 229466]
plt.xlabel('Number of users')
plt.xticks(x)
plt.plot(x, y, 'r')
plt.yticks(np.arange(0, 200, 50))
plt.show()


# 1 mill posts, complex policies
plt.ylabel('Memory usage (kB)')
x = [10, 100, 1000]

plt.xlabel('Number of users')
plt.xticks([10, 100, 1000])
plt.plot(x, y, 'r')
plt.xscale('log')
plt.yticks(np.arange(1800, 2100, 100))
plt.show()

#
# plt.ylabel('Write throughput (1000 PUTs/sec)')
# plt.xlabel('Number of users')
# plt.show()
