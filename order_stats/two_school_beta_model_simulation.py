import multiprocess as mp
import sys
import csv


def run_all(num, p):
    model_alpha_parameter = 2.5
    model_beta_parameter = 4
    model_scale_parameter = 19
    model_shift_parameter = 5.7
    n1 = 1728
    n2 = 433

    import numpy as np

    def run(num, p):

        x_list = []
        y_list = []

        percent = 0

        for i in range(num):
            if i%(num/100) == num/100 - 1:
                percent += 1
                print("process {}: {}%".format(p, percent))

            x_temp = np.random.beta(model_alpha_parameter, model_beta_parameter, n1)
            y_temp = np.random.beta(model_alpha_parameter, model_beta_parameter, n2)

            x_list.append(sorted(x_temp[np.argpartition(x_temp, 7)[:7]] * model_scale_parameter + model_shift_parameter))
            y_list.append(sorted(y_temp[np.argpartition(y_temp, 7)[:7]] * model_scale_parameter + model_shift_parameter))

        return x_list, y_list
    
    return run(num, p)


if __name__ == "__main__":  
    pool = mp.Pool(processes=mp.cpu_count())

    temp = []

    for i in range(int(sys.argv[1])):
        temp.append(pool.apply_async(run_all, (int(sys.argv[2]), i,)))

    pool.close()
    pool.join()
    
    print("simulation finish")

    x_file = open("x_result.csv", "a+", newline='')
    y_file = open("y_result.csv", "a+", newline='')

    x_writer = csv.writer(x_file)
    y_writer = csv.writer(y_file)
    
    for i in temp[:]:
        data_temp = i.get()
        for data in data_temp[0]:
            x_writer.writerow(data)
        for data in data_temp[1]:
            y_writer.writerow(data)

    x_file.close()
    y_file.close()
