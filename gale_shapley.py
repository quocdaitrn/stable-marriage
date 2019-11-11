import utils
import copy
from numpy import array


def gs_find_optimal_and_shortlist(preference_lists: list, opposite_sex_preference_lists: list, is_men_oriented: bool) -> dict:
    """
    Stable marriage problem with the Gale-Shapley algorithm

    :param preference_lists: preference lists
    :param opposite_sex_preference_lists: opposite sex's preference lists
    :param is_men_oriented: flag to determine men oriented or women oriented

    :return: dictionary with 3 field short lists, opposite sex's short lists and optimal
    """

    n = len(preference_lists)

    short_lists = copy.deepcopy(preference_lists)
    opposite_sex_short_lists = copy.deepcopy(opposite_sex_preference_lists)

    # M(i) contains the most preferable person of opposite sex
    M = [-1] * n
    opposite_sex_matching = [-1] * n

    # rank_w(i) contains the position of the searching person of opposite sex
    rank_w = [0 for _ in range(n)]

    # Gale-Shapley algorithm
    while True:
        # find a person who is free
        free_person = [i for i, val in enumerate(M) if val == -1]

        # there is not any available person then stop
        if len(free_person) == 0:
            break

        # choose the first person
        mi = free_person[0]

        # find the first next person of opposite sex's rank from m's preference list
        mr = rank_w[mi]

        # person m process person of opposite sex w(i)
        wi = preference_lists[mi][mr]

        # if person of opposite sex opposite_sex_matching(wi) is free then it becomes engaged
        if opposite_sex_matching[wi] == -1:
            M[mi] = wi
            opposite_sex_matching[wi] = mi
            rank_w[mi] = rank_w[mi] + 1
            # remove people mj following mi from wj's list and also wi from wj
            wr = utils.find(opposite_sex_preference_lists[wi], mi)
            for mj in range(wr + 1, n):
                opposite_sex_short_lists[wi][mj] = -1
                wj = utils.find(preference_lists[opposite_sex_preference_lists[wi][mj]], wi)
                short_lists[opposite_sex_preference_lists[wi][mj]][wj] = -1
        else:
            # if opposite_sex_matching(i) is currently engaged to mj = opposite_sex_matching(wi)
            mj = opposite_sex_matching[wi]
            mj_rank = utils.find(opposite_sex_preference_lists[wi], mj)
            mi_rank = utils.find(opposite_sex_preference_lists[wi], mi)
            # compare the ranks of mi and mj
            if mi_rank < mj_rank:
                M[mj] = -1
                M[mi] = wi
                opposite_sex_matching[wi] = mi
                # remove people mj following mi from wj's list and also wi from wj
                for mj in range(mi_rank + 1, n):
                    opposite_sex_short_lists[wi][mj] = -1
                    wj = utils.find(preference_lists[opposite_sex_preference_lists[wi][mj]], wi)
                    short_lists[opposite_sex_preference_lists[wi][mj]][wj] = -1

            rank_w[mi] = rank_w[mi] + 1

    short_lists_normalize = [[x + 1 for x in row] for row in short_lists]
    opposite_sex_short_lists_normalize = [[x + 1 for x in row] for row in opposite_sex_short_lists]

    if is_men_oriented:
        # M_normalize = [x + 1 for x in M]
        M_normalize = M
    else:
        # M_normalize = [x + 1 for x in opposite_sex_matching]
        M_normalize = opposite_sex_matching

    return {"short_lists": short_lists, "opposite_sex_short_lists": opposite_sex_short_lists,
            "M": M_normalize}


# result_man = gs_find_optimal_and_shortlist(utils.read_file("men19viet.txt"), utils.read_file("women19viet.txt"), True)
#
#
# print(f'opposite_sex_short_lists: {array(result_man["opposite_sex_short_lists"])}')
# print(f'short_lists: {array(result_man["short_lists"])}')
# print(f'Man-optimal: {result_man["M"]}')
#
# test = copy.deepcopy(result_man["M"])
# test[0] = 100
#
# print(f'test:{result_man["M"]}')
#
#
# result_woman = gs_find_optimal_and_shortlist(utils.read_file("women19viet.txt"), utils.read_file("men19viet.txt"), False)
#
# print(f'opposite_sex_short_lists: {array(result_woman["opposite_sex_short_lists"])}')
# print(f'short_lists: {array(result_woman["short_lists"])}')
# print(f'Woman-optimal: {result_woman["M"]}')
#
# a=[1, 2, 4, 6]
# print(a[:3])