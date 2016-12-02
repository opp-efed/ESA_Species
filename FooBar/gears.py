import numpy as np
from numpy.linalg import inv
from fractions import Fraction


def answer(pegs):
    matrix = []
    row_n=0
    delta =[]

    for loc in pegs:
        try:
            delta.append((pegs[row_n+1]- pegs[row_n]))
            # if row_n==0:
            #     print Fraction(2)
            #     row= [(2),(1),0]
            #     matrix.append(row)
            # elif row_n ==len(pegs)-2:
            #     row= [1,0,(len(pegs)-3+1)]
            #     matrix.append(row)
            #     break
            # else:
            #     row= [0,1,1]
            #     print row
            #     matrix.append(row)
            row_n+=1
        except:
            pass
    matrix = [[2,1,0],[1,0,1],[0,1,1]]
    print matrix
    inverse_matrix = inv(matrix)
    inverse_matrix= [Fraction(inverse_matrix[0][0]),Fraction(inverse_matrix[0][1]),Fraction(inverse_matrix[0][2])],\
                    [Fraction(inverse_matrix[1][0]),Fraction(inverse_matrix[1][1]),Fraction(inverse_matrix[01][2])],\
                    [Fraction(inverse_matrix[2][0]),Fraction(inverse_matrix[2][1]),Fraction(inverse_matrix[2][2])]
    inverse_matrix= [Fraction(inverse_matrix[0][0]),Fraction(inverse_matrix[0][1]),Fraction(inverse_matrix[0][2])],\
                    [Fraction(inverse_matrix[1][0]),Fraction(inverse_matrix[1][1]),Fraction(inverse_matrix[01][2])],\
                    [Fraction(inverse_matrix[2][0]),Fraction(inverse_matrix[2][1]),Fraction(inverse_matrix[2][2])]
    for i in range(1,(len(pegs)-1)):
        y = Fraction(0)
        for j in range(len(pegs)-1):
            y= y+inverse_matrix[i][j]* delta[j]
        # if (y.numerator<1) or (y.numerator<y.denominator):
        #     return [-1,-1]
    x =Fraction(0)
    for i in range(1,len(pegs)-1):
        x= x+ inverse_matrix[0][1]*delta[i]
    x= x *Fraction(2)
    # if (x.numerator<1) or (x.numerator<x.denominator):
    #         return [-1,-1]
    return [x.numerator,x.denominator]




    # 2x +a = equ1
    # val_1 = peg[1]- peg[0]
    # #a+b = equ2
    # val_2 = peg[2]- peg[1]
    # #x+b = equ3
    # val_3 = peg[3]-peg[2]
    #
    # matrix_equ = np.array([[2,1,0],[0,1,1],[1,0,1]])
    # matrix_val =  np.array([[val_1],[val_2],[val_3]])
    # print matrix_equ
    # inverse_equ = (matrix_equ)
    # print inverse_equ
    # matrix_results = inverse_equ*matrix_val
    #
    # print matrix_results
print answer([4,30,50])