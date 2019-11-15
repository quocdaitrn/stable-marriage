from utils import find
import copy


def break_marriage_man(men_short_lists: list, women_short_lists: list, M: list, m: int, Mt: list) -> list:
    n = len(men_short_lists)
    stable_matching = []

    # pre-processing for restarting of Gale-Shapley
    women_matching = [-1] * n

    # rank of men from women
    for i in range(8):
        # print(f'M_Man: {M}')
        women_matching[M[i]] = i

    # rank_w contains the position of the searching women
    rank_w = [0 for _ in range(n)]
    for i in range(n):
        pos = find(men_short_lists[i], M[i])
        rank_w[i] += pos + 1

    # break m
    wi = M[m]
    M[m] = -1

    # restart Gale-Shapley
    found = False
    while True:
        # find the man mi who is free
        mi = find(M, -1)
        # break man mi
        if -1 < mi < m:
            break
        # there is not any available person then stop
        if mi == -1:
            found = True
            break
        # rank of women wj in man mi's list
        mr = rank_w[mi]
        if mr >= n:
            break
        if men_short_lists[mi][mr] > -1:
            last_r = find(men_short_lists[mi], Mt[mi])
            if mr > last_r:
                break
            # wj in man mi's list and its partner
            wj = men_short_lists[mi][mr]
            mj = women_matching[wj]
            # rank of man mj in woman wj's list
            mj_rank = find(women_short_lists[wj], mj)
            # rank of current man mi in woman wj's list
            mi_rank = find(women_short_lists[wj], mi)
            if mi_rank < mj_rank:
                M[mi] = wj
                women_matching[wj] = mi
                if wj != wi:
                    M[mj] = -1

        rank_w[mi] += 1

    if found:
        stable_matching = M

    return stable_matching


# result = break_marriage_man(utils.read_file("menshortlists.txt"), utils.read_file("womenshortlists.txt"),
#                    [3, 2, 7, 4, 0, 5, 1, 6], 2, [0, 3, 6, 7, 2, 4, 5, 1])
#
# print(result)

def break_marriage_woman(men_short_lists: list, women_short_lists: list, M: list, m: int, M0: list) -> list:
    n = len(men_short_lists)
    stable_matching = []

    # pre-processing for restarting of Gale-Shapley
    women_matching = [-1] * n
    women_M0 = [-1] * n

    # rank of men from women
    for i in range(8):
        # print(f'M_Woman: {M}')
        women_matching[M[i]] = i
        women_M0[M0[i]] = i

    # exchange the role of men and women
    temp_M = copy.deepcopy(M)
    M = copy.deepcopy(women_matching)
    women_matching = copy.deepcopy(temp_M)

    # rank_w contains the position of the searching women
    rank_w = [0 for _ in range(n)]
    for i in range(n):
        pos = find(men_short_lists[i], M[i])
        rank_w[i] += pos + 1

    # break m
    wi = M[m]
    M[m] = -1

    # restart Gale-Shapley
    found = False
    while True:
        # find the man mi who is free
        mi = find(M, -1)
        # break man mi
        if -1 < mi < m:
            break
        # there is not any available person then stop
        if mi == -1:
            found = True
            break
        # rank of women wj in man mi's list
        mr = rank_w[mi]
        if mr >= n:
            break
        if men_short_lists[mi][mr] > -1:
            last_r = find(men_short_lists[mi], women_M0[mi])
            if mr > last_r:
                break
            # wj in man mi's list and its partner
            wj = men_short_lists[mi][mr]
            mj = women_matching[wj]
            # rank of man mj in woman wj's list
            mj_rank = find(women_short_lists[wj], mj)
            # rank of current man mi in woman wj's list
            mi_rank = find(women_short_lists[wj], mi)
            if mi_rank < mj_rank:
                M[mi] = wj
                women_matching[wj] = mi
                if wj != wi:
                    M[mj] = -1

        rank_w[mi] += 1

    if found:
        stable_matching = women_matching

    return stable_matching


# result = break_marriage_woman(utils.read_file("menshortlists.txt"), utils.read_file("womenshortlists.txt"),
#                               [0, 3, 6, 7, 2, 4, 5, 1], 7, [3, 2, 7, 4, 0, 5, 1, 6])
#
# print(result)