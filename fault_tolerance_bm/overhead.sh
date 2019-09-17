HOME="/home/gina"
RESULTS_DIR="$HOME/noria/fault_tolerance_bm/results_gina"

build() {
    BRANCH=$1
    BINARY=$2

    git checkout $BRANCH
    pls cargo b --release --bin vote && pls cp $HOME/noria/target/release/vote $HOME/noria/bin/$BINARY
    echo "built branch $BRANCH as binary $BINARY"
}

build_all()
{
    # need to create ft_no_clone by cherry-picking 3425981f onto wpbm_ft_sharded
    build wpbm_ft_sharded ft
    build wpbm_master_sharded master
}

# build wpbm_ft_sharded ft
# build wpbm_master_sharded master
sharding=20
truncate_every=2
for target in $(seq 100 100 5000)
do
    ./run_localsoup.sh master $(($target * 1000)) $sharding 1
    ./run_localsoup.sh ft_no_clone $(($target * 1000)) $sharding $truncate_every
    ./run_localsoup.sh ft $(($target * 1000)) $sharding $truncate_every
done

# for target in $(seq 100 100 5000)
# do
#     truncate_every=2
#     ./run_localsoup.sh master $(($target * 1000)) $sharding 1
#     ./run_localsoup.sh ft_no_clone $(($target * 1000)) $sharding $truncate_every
#     # ./run_localsoup.sh ft $(($target * 1000)) $sharding $truncate_every
# done

# for target in $(seq 20000 20000 400000)
# do
#     for sharding in 1 4 10
#     do
#         ./run_localsoup.sh master $target $sharding 1
#         for truncate_every in 2
#         do
#             ./run_localsoup.sh ft $target $sharding $truncate_every
#         done
#     done
# done

# for target in 1000000
# do
#     for sharding in 10 20
#     do
#         for truncate_every in 1 2 4 8
#         do
#             ./run_localsoup.sh ft $target $sharding $truncate_every
#         done
#     done
# done

# for target in 100000
# do
#     for sharding in 1 2 4 8 10
#     do
#         for truncate_every in 2
#         do
#             ./run_localsoup.sh ft $target $sharding $truncate_every
#         done
#     done
# done
