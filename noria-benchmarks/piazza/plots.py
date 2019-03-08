import matplotlib.pyplot as plt
import numpy as np


# exp setup
nusers = [1000, 10000]
nclasses = [100]
nposts = [10, 100]

txput_dict = dict() # (nusers, nclasses, nposts) -> txput in GETs/sec
for users in nusers:
    for classes in nclasses:
        for posts in nposts:
            postcount = classes * posts
            fname = "exp-{}-{}-{}.txt".format(users, posts, classes)
            with open(fname) as f:
                content = f.readlines()
                txput = content[-2]
                txput = txput.split(" ")
                txput = txput[-2]
                txput = txput.replace("(", "")
                txput = txput.replace(" ", "")
                print("Experiment {} users {} classes {} posts/class read throughput: {}".format( users, classes, posts, txput))
                try:
                    txput_dict[(users, classes, posts)] = float(txput)
                except:
                    print("Experiment results for {} users {} classes {} posts/class not parseable, something went wrong!".format(users, classes, posts))


for users in nusers:
    ngroups = len(nclasses) * len(nposts)
    mean_txputs = [[] for i in range(ngroups)]

    fig, ax = plt.subplots()

    index = np.arange(ngroups)
    opacity = 0.4
    bar_width = 0.35

    labels = []
    offset = index
    txputs = []

    for classes in nclasses:
        for posts in nposts:
            postcount = classes * posts
            try:
                tx = txput_dict[(users, classes, posts)]
                if tx < 10000 or tx > 1000000:
                    print("Experiment with {} users {} classes {} posts/class had unexpected throughput results ({} GETs/sec)!".format(users, classes, posts, tx))
                txputs.append(txput_dict[(users, classes, posts)])
                labels.append("{} classes, {} posts".format(classes, postcount))
            except:
                txputs.append(0)
                labels.append("Experiment with {} users {} classes {} posts/class failed!".format(users, classes, posts))

    ax.bar(index, txputs)
    ax.set_xlabel('Configuration')
    ax.set_ylabel('Read Throughput (GETs/sec)')
    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels(labels)

    fig.suptitle('{} Users'.format(users), fontsize=20)
    fig.tight_layout()

    plt.savefig('{}_users_read_txput_results.png'.format(users))
    plt.show()
