# from ggplot import * 
# import pandas as pd


def main():
    nusers = [100, 1000, 10000]
    ntweets = [1000, 10000, 100000]
    exps = []
    for nuser in nusers: 
        for ntweet in ntweets: 
            fname = 'exp-clientmat-{}nusers-{}ntweets.txt'.format(nuser, ntweet)
            try: 
                f = open(fname, 'r+')
                lines = f.readlines() 
                multiple = float(lines[3].split()[1].strip('"'))
                latency = float(lines[4].strip('"\n')) 
                exps.append((nuser, ntweet, multiple, latency))

            except Exception as e: 
                print(e)
                continue 

    for exp in exps: 
        print("{} users, {} tweets, memory overhead: {}x, timeline query latency: {}".format(exp[0], exp[1], exp[2], exp[3]))
   
    # df = pd.DataFrame({'a': range(10), 'b': range(5, 15), 'c': range(7, 17)})
    # df['x'] = df.index
    # ggplot(aes(x='x'), data=df) +\
    #     geom_line(aes(y='a'), color='blue') +\
    #     geom_line(aes(y='b'), color='red') +\
    #     geom_line(aes(y='c'), color='green')
        


if __name__ == '__main__': 
    main() 