# This program is to simulate the result of state meet by using the hypothesized distribution of win rate
# The result will be used to do the chi-square test to check if the events are independent

import multiprocess as mp
import pandas as pd
import numpy as np
import sys
import random


def test(repeat, desired, p, mp, pd, random, np):
    """
    this function is used to simulate the frequency of different combinations of outcomes of which schools get into top 16
    params:
        repeat: how many time of repeat
        desired: which schools are the users want to simulate
        p: number of process
        mp: multiprocess
        pd: pandas
        random: random
        np: numpy
    return:
        freq3: 
        freq2:
        freq1:
    """
    def run_once(desired):
        """
        this function calculate the result of single running match
        params:
            desired: a list of school ID, each time we use desired[0] for one school simulation and desired[0:1] for two schools and all of them for three schools
        return:
            ret: a list of result for the simulation for one school, two schools and three schools
        """

        ret = []

        # current_all is a dataframe include schools in division one
        current_all = pd.read_csv(r"allxc.csv")
        current_all = current_all[current_all["div"] == "I"]
        current_all = current_all.sort_values("pop").reset_index().loc[:, "ID":"pop"]

        temp = np.empty(shape=16, dtype=np.int64)
        current_total_pop = current_all["pop"].sum()
        count = 0

        # repeat 16 times to simulate the top 16 runners
        for _ in range(16):
            rand_num = random.random()
            cumulative_prob = 0
            for index in current_all.index:
                current_prob = cumulative_prob + current_all.loc[index, "pop"] / current_total_pop
                # if the proportion of population is in the range then we think the runner get into top 16
                if cumulative_prob <= rand_num < current_prob:
                    temp[count] = index
                    count += 1
                    current_total_pop -= current_all.loc[index, "pop"]
                    current_all = current_all.drop(index)
                    break
                cumulative_prob = current_prob

        # different combination of runners get into top 16
        # freq1:
        #     0: True
        #     1: False
        # freq2:
        #     0: s1 and s2
        #     1: s1
        #     2: s2
        #     3: not s1 and not s2
        # freq3:
        #     0: neither
        #     1: s3
        #     2: s2
        #     3: s2 & s3
        #     4: s1
        #     5: s1 & s3
        #     6: s2 & s3
        #     7: s1 & s2 & s3

        if np.isin(desired[0], temp) and np.isin(desired[1], temp) and np.isin(desired[2], temp):
            ret.append(7)
        elif np.isin(desired[0], temp) and np.isin(desired[1], temp):
            ret.append(6)
        elif np.isin(desired[1], temp) and np.isin(desired[2], temp):
            ret.append(3)
        elif np.isin(desired[0], temp) and np.isin(desired[2], temp):
            ret.append(5)
        elif np.isin(desired[0], temp):
            ret.append(4)
        elif np.isin(desired[1], temp):
            ret.append(2)
        elif np.isin(desired[2], temp):
            ret.append(1)
        else:
            ret.append(0)

        if np.isin(desired[0], temp) and np.isin(desired[1], temp):
            ret.append(0)
        elif np.isin(desired[0], temp):
            ret.append(1)
        elif np.isin(desired[1], temp):
            ret.append(2)
        else:
            ret.append(3)

        if np.isin(desired[0], temp):
            ret.append(0)
        else:
            ret.append(1)

        if np.isin(desired[1], temp):
            ret.append(0)
        else:
            ret.append(1)

        if np.isin(desired[2], temp):
            ret.append(0)
        else:
            ret.append(1)
        return ret
        
    percent = 0

    freq3 = {i: [0] for i in range(8)}
    freq2 = {i: [0] for i in range(4)}
    freq1_1 = {i: [0] for i in range(2)}
    freq1_2 = {i: [0] for i in range(2)}
    freq1_3 = {i: [0] for i in range(2)}

    for _ in range(repeat):

        res = run_once(desired)

        freq3[res[0]][0] += 1
        freq2[res[1]][0] += 1
        freq1_1[res[2]][0] += 1
        freq1_2[res[3]][0] += 1
        freq1_3[res[4]][0] += 1
        if _%(repeat/100) == repeat/100 - 1 and p == 1:
            percent += 1
            print("progress: {}%".format(percent))
    return freq3, freq2, freq1_1, freq1_2, freq1_3


if __name__ == '__main__':
    """
    this program use three system:
    sys.argv: repeat, repeat_simulation_once, target[s1, s2, s3] <- s1,s2,s3 are the index of schools in the DataFrame only include schools from div I
    """

    current_all = pd.read_csv(r"allxc.csv")
    current_all = current_all[current_all["div"] == "I"]
    current_all = current_all.sort_values("pop").reset_index().loc[:, ["ID","pop"]]

    desired = np.array([int(i) for i in sys.argv[3][1:-1].split(",")], dtype=np.int64)
    print(current_all.iloc[desired])

    print("\n\n")
    print("simulation start")

    temp = []   
    pool = mp.Pool(processes=mp.cpu_count())
    

    for i in range(int(sys.argv[1])):
        temp.append(pool.apply_async(test, (int(sys.argv[2]), desired, i, mp, pd, random, np)))

    pool.close()
    pool.join()

    df3 = pd.DataFrame({i: [] for i in range(8)})
    df2 = pd.DataFrame({i: [] for i in range(4)})
    df1_1 = pd.DataFrame({i: [] for i in range(2)})
    df1_2 = pd.DataFrame({i: [] for i in range(2)})
    df1_3 = pd.DataFrame({i: [] for i in range(2)})
    
    for i in temp:
        df3 = pd.concat([df3, pd.DataFrame(i.get()[0])], ignore_index=True)
        df2 = pd.concat([df2, pd.DataFrame(i.get()[1])], ignore_index=True)
        df1_1 = pd.concat([df1_1, pd.DataFrame(i.get()[2])], ignore_index=True)
        df1_2 = pd.concat([df1_2, pd.DataFrame(i.get()[3])], ignore_index=True)
        df1_3 = pd.concat([df1_3, pd.DataFrame(i.get()[4])], ignore_index=True)

    df3.columns = ["none", "S2", "S1", "S1&S2", "S0", "S0&S2", "S0&S1", "all"]
    df2.columns = ["S1&S2", "S1", "S2", "neither"]
    df1_1.columns = ["True", "False"]
    df1_2.columns = ["True", "False"]
    df1_3.columns = ["True", "False"]
 
    for col in df3.columns:
        df3[col].astype("int64")
    for col in df2.columns:
        df2[col].astype("int64")
    for col in df1_1.columns:
        df1_1[col].astype("int64")
    for col in df1_2.columns:
        df1_2[col].astype("int64")
    for col in df1_3.columns:
        df1_3[col].astype("int64")
    
    df3.to_csv("ind3.csv", index=False)
    df2.to_csv("ind2.csv", index=False)
    df1_1.to_csv("ind1_1.csv", index=False)
    df1_2.to_csv("ind1_2.csv", index=False)
    df1_3.to_csv("ind1_3.csv", index=False)

    print("\n\n")
    print("simulation finish")