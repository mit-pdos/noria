import matplotlib.pyplot as plt
import numpy as np


# plt.ylabel('Write throughput (1000 PUTs/sec)')
# plt.xlabel('Number of users')
# plt.show()
#


# 1 mill posts, complex policies
plt.ylabel('Memory usage (kB)')
x = [10, 100, 1000]
y = [1839, 1892, 1994]
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
