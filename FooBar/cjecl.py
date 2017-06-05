def answer(data, n):
    task_dict = {}
    for i in list(data):

        pre_seen = task_dict.get(i)


        if pre_seen == None:
            task_dict[i] = 1
        else:
            current_count = int(task_dict[i])
            current_count += 1

            task_dict[i]= current_count
    print task_dict


    revised_list2= [k for  k,v in task_dict.iteritems() if v <=n]
    v =task_dict[10]
    print int(task_dict[10])
    revised_list2= [i for i in data if (int(task_dict[i]) <=n )]

    return  revised_list2

check =answer([5,10,15,10,7],3)
print check

