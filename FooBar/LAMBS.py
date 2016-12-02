
def answer(lambs):
    min_hench = [1,2]
    max_hench = [1,1]
    max_bool = True

    while max_bool:
        min_counter = len(min_hench)
        max_counter =len(max_hench)

        max_step =  max_hench[max_counter-2] +max_hench[max_counter-1]
        min_step = min_hench[min_counter-1] * 2
        if sum(max_hench) + max_step<=lambs:
            max_hench.append(max_step)
        else:
            max_bool= False

        if sum(min_hench) + min_step <= lambs:
            min_hench.append(min_step)


    return(int(len(max_hench)- len(min_hench)))






print answer(143)
