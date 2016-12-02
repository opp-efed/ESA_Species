
def answers(x,y):
    check_x = [i for i in x if i not in y]
    check_y =[j for j in y if j not in x]
    if len(check_x) != 0:
        return  check_x[0]
    elif len(check_y) != 0:
        return check_y[0]

    print len(check_x)
    print len(check_y)

x= [14,27,1,4,2,50,3,1]
y= [2,4,-4,3,1,1,14,27,50]

result = answers(x,y)
print result