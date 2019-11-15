from random import random, randint

from mpi4py import MPI

import utils
import copy

from gale_shapley import gs_find_optimal_and_shortlist
from operations import break_marriage_man, break_marriage_woman


def shortl_bils():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    global sm1, sm2

    # define man preference list
    men_prefenrence_lists = utils.read_file("men8.txt")
    women_prefenrence_lists = utils.read_file("women8.txt")

    # man optimal and woman optimal solution
    gs_man_result = gs_find_optimal_and_shortlist(men_prefenrence_lists, women_prefenrence_lists, is_men_oriented=True)
    men_shortlists_0 = copy.deepcopy(gs_man_result["short_lists"])
    women_shortlists_0 = copy.deepcopy(gs_man_result["opposite_sex_short_lists"])
    M0 = copy.deepcopy(gs_man_result["M"])

    gs_woman_result = gs_find_optimal_and_shortlist(women_prefenrence_lists, men_prefenrence_lists,
                                                    is_men_oriented=False)
    women_shortlists_t = copy.deepcopy(gs_woman_result["short_lists"])
    men_shortlists_t = copy.deepcopy(gs_woman_result["opposite_sex_short_lists"])
    Mt = copy.deepcopy(gs_woman_result["M"])

    # merge shortlists to obtain the better shortlists
    # the size of SMP
    n = len(men_shortlists_0)
    men_shortlists = [[-1 for _ in range(n)] for _ in range(n)]
    women_shortlists = [[-1 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if men_shortlists_0[i][j] == men_shortlists_t[i][j]:
                men_shortlists[i][j] = men_shortlists_0[i][j]
            if women_shortlists_0[i][j] == women_shortlists_t[i][j]:
                women_shortlists[i][j] = women_shortlists_0[i][j]

    # initialize the solution
    M_left = copy.deepcopy(M0)
    fM_left, _, _ = utils.matching_cost(men_shortlists, women_shortlists, M_left)
    M_right = copy.deepcopy(Mt)
    fM_right, _, _ = utils.matching_cost(men_shortlists, women_shortlists, M_right)
    if fM_left < fM_right:
        M_best = copy.deepcopy(M_left)
        fM_best = fM_left
    else:
        M_best = copy.deepcopy(M_right)
        fM_best = fM_right

    # initialize the propability
    p = 0.05
    forward = True
    backward = True
    neighbor_left = [copy.deepcopy(M0)]
    neighbor_right = [copy.deepcopy(Mt)]
    k = 1
    t = 1
    mans_per_each_process = int(n / size)
    is_top = False
    while True:
        if is_top:
            break
        # -------------------search forward---------------------
        if forward:
            neigbor_set = []
            for h in range(len(neighbor_left)):
                M_temp_man = copy.deepcopy(neighbor_left[h])
                bml = break_marriage_list(rank * mans_per_each_process, (rank + 1) * mans_per_each_process,
                                          copy.deepcopy(men_shortlists),
                                          copy.deepcopy(women_shortlists),
                                          copy.deepcopy(M_temp_man),
                                          copy.deepcopy(Mt)
                                          )
                neigbor_set_tmp = comm.reduce(bml, root=0)
                print(f'temp: {neigbor_set_tmp}')
                if rank == 0:
                    neigbor_set.extend(neigbor_set_tmp)

            print(neigbor_set)
            comm.bcast()
            if len(neigbor_set) != 0:
                # find the best neighbor matchings
                neighbor_cost = []
                for i in range(len(neigbor_set)):
                    M_child = copy.deepcopy(neigbor_set[i])
                    fM_child, _, _ = utils.matching_cost(men_shortlists, women_shortlists, M_child)
                    neighbor_cost.append(fM_child)
                if random() <= p:
                    j = randint(0, len(neigbor_set) - 1)
                    index_arr = [j]
                else:
                    index_arr = sorted(range(len(neighbor_cost)), key=lambda idx: neighbor_cost[idx])
                    j = index_arr[0]
                M_next = copy.deepcopy(neigbor_set[j])
                fM_next, sm1, _ = utils.matching_cost(men_shortlists, women_shortlists, M_next)
                fM_left, _, _ = utils.matching_cost(men_shortlists, women_shortlists, M_left)
                if fM_next >= fM_left:
                    forward = False
                    # remember the best solution
                    if fM_best > fM_left:
                        M_best = copy.deepcopy(M_left)
                        fM_best = fM_left

                # for next iteration
                M_left = copy.deepcopy(M_next)
                if len(index_arr) == 1:
                    neighbor_left = copy.deepcopy(neigbor_set[index_arr[0]])
                else:
                    if k > len(neigbor_set):
                        neighbor_left = copy.deepcopy(neigbor_set)
                    else:
                        neighbor_left = neigbor_set[:k]
            else:
                forward = False

        # -------------------search backward---------------------
        if rank == 0:
            if backward:
                neigbor_set = []
                for h in range(len(neighbor_right)):
                    M_temp_woman = copy.deepcopy(neighbor_right[h])
                    for m in range(n):
                        M_child = break_marriage_woman(women_shortlists, men_shortlists, copy.deepcopy(M_temp_woman), m,
                                                       copy.deepcopy(M0))
                        if len(M_child) != 0:
                            neigbor_set.append(copy.deepcopy(M_child))
                if len(neigbor_set) != 0:
                    # find the best neighbor matchings
                    neighbor_cost = []
                    for i in range(len(neigbor_set)):
                        M_child = copy.deepcopy(neigbor_set[i])
                        fM_child, _, _ = utils.matching_cost(men_shortlists, women_shortlists, M_child)
                        neighbor_cost.append(fM_child)
                    if random() <= p:
                        j = randint(0, len(neigbor_set) - 1)
                        index_arr = [j]
                    else:
                        index_arr = sorted(range(len(neighbor_cost)), key=lambda idx: neighbor_cost[idx])
                        j = index_arr[0]
                    M_next = neigbor_set[j]
                    fM_next, sm2, _ = utils.matching_cost(men_shortlists, women_shortlists, M_next)
                    fM_right, _, _ = utils.matching_cost(men_shortlists, women_shortlists, M_left)
                    if fM_next >= fM_right:
                        backward = False
                        # remember the best solution
                        if fM_best > fM_right:
                            M_best = copy.deepcopy(M_right)
                            fM_best = fM_right

                    # for next iteration
                    M_right = copy.deepcopy(M_next)
                    if len(index_arr) == 1:
                        neighbor_right = copy.deepcopy(neigbor_set[index_arr[0]])
                    else:
                        if k > len(neigbor_set):
                            neighbor_right = copy.deepcopy(neigbor_set)
                        else:
                            neighbor_right = neigbor_set[:k]
                else:
                    backward = False

            if (not forward) and (not backward):
                if sm1 <= sm2:
                    forward = True
                    backward = True
                else:
                    break

            t = t + 1

    if rank == 0:
        print(M_best)


def break_marriage_list(start, end, men_shortlists, women_shortlists, M, Mt):
    neigbor_set = []
    for m in range(start, end):
        M_child = break_marriage_man(men_shortlists, women_shortlists, M, m, Mt)
        if len(M_child) != 0:
            neigbor_set.append(copy.deepcopy(M_child))

    return neigbor_set


shortl_bils()
