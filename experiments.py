import subprocess
import argparse
import matplotlib.pyplot as plt
import numpy as np


def plot_results(output_path, nusers, nposts, nclasses):
    txput_dict = dict() # (nusers, nclasses, nposts) -> txput in GETs/sec
    for users in nusers:
        for classes in nclasses:
            for posts in nposts:
                postcount = classes * posts
                fname = "{}/exp-{}u-{}ppc-{}c.txt".format(output_path, users, posts, classes)
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

        plt.savefig('{}/{}_users_read_txput_results.png'.format(output_path, users))


def run_experiments(output_path, nusers, nposts, nclasses):
    for users in nusers:
        for classes in nclasses:
            for posts in nposts:
                postcount = posts * classes
                cmd = "cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l {} -p {} -u {} -c {} --policies noria-benchmarks/piazza/basic-policies.json > {}/exp-{}u-{}ppc-{}c.txt".format(users, postcount, users, classes, output_path, users, posts, classes)
                print("Running command: {}".format(cmd))
                subprocess.call(cmd, shell=True)

    plot_results(output_path, nusers, nposts, nclasses)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_path', type=str,
                        help='path to output folder')
    parser.add_argument('--num_users', type=str,
                        help='number of users, separated by commas if more than one')
    parser.add_argument('--num_posts_per_class', type=str,
                        help='number of posts per class, separated by commas if more than one')
    parser.add_argument('--num_classes', type=str,
                        help='number of classes, separated by commas if more than one')

    args = parser.parse_args()

    nusers = list(map(int, args.num_users.split(',')))
    nposts = list(map(int, args.num_posts_per_class.split(',')))
    nclasses = list(map(int, args.num_classes.split(',')))

    print("nusers: {} nposts: {} nclasses: {}".format(nusers, nposts, nclasses))
    run_experiments(args.output_path, nusers, nposts, nclasses)


if __name__ == "__main__":
    main()
