from mpi4py import MPI
from mpi_master_slave import Master, Slave
from mpi_master_slave import WorkQueue

from random import random, randint
import time
import copy

import utils
from gale_shapley import gs_find_optimal_and_shortlist
from operations import break_marriage_man


class App(object):
    """
    This is my application that has a lot of work to do so it gives work to do
    to its slaves until all the work is done
    """

    def __init__(self, slaves):
        # when creating the Master we tell it what slaves it can handle
        self.master = Master(slaves)
        # WorkQueue is a convenient class that run slaves on a tasks queue
        self.work_queue = WorkQueue(self.master)
        self.neigbor_set = []

    def get_neigbor_set(self):
        """
        Get set of neighbors
        """
        return self.neigbor_set

    def terminate_slaves(self):
        """
        Call this to make all slaves exit their run loop
        """
        self.master.terminate_slaves()

    def run(self, l: list):
        """
        This is the core of my application, keep starting slaves
        as long as there is work to do
        """
        #
        # let's prepare our work queue. This can be built at initialization time
        # but it can also be added later as more work become available
        #
        for i in range(len(l)):
            # 'data' will be passed to the slave and can be anything
            self.work_queue.add_work(data=l[i])

        #
        # Keep starting slaves as long as there is work to do
        #
        while not self.work_queue.done():
            #
            # give more work to do to each idle slave (if any)
            #
            self.work_queue.do_work()

            #
            # reclaim returned data from completed slaves
            #
            for slave_return_data in self.work_queue.get_completed_work():
                done, child, message = slave_return_data
                if done:
                    print('Master: slave finished is task and says "%s"' % message)
                    if (child is not None) and (len(child) != 0):
                        self.neigbor_set.append(copy.deepcopy(child))

            # sleep some time: this is a crucial detail discussed below!
            time.sleep(0.03)


class Worker(Slave):
    """
    A slave process extends Slave class, overrides the 'do_work' method
    and calls 'Slave.run'. The Master will do the rest
    """

    def __init__(self):
        super(Worker, self).__init__()

    def do_work(self, data):
        rank = MPI.COMM_WORLD.Get_rank()
        name = MPI.Get_processor_name()
        men_shortlists, women_shortlists, M, m, Mt = data

        result = break_marriage_man(men_shortlists, women_shortlists, M, m, Mt)

        print(f'  Slave {name} rank {rank} is executing')

        return True, result, f'Slave at rank {rank} completed its task'


def main():
    start = time.time()

    name = MPI.Get_processor_name()
    rank = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()

    if rank == 0:  # Master
        global sm1, sm2

        # read input
        men_prefenrence_lists, women_prefenrence_lists = get_preference_lists("input/men8.txt", "input/women8.txt")

        # man optimal and woman optimal solution
        gs_man_result = gs_find_optimal_and_shortlist(men_prefenrence_lists, women_prefenrence_lists,
                                                      is_men_oriented=True)
        men_shortlists_0 = copy.deepcopy(gs_man_result["short_lists"])
        women_shortlists_0 = copy.deepcopy(gs_man_result["opposite_sex_short_lists"])
        M0 = copy.deepcopy(gs_man_result["M"])

        gs_woman_result = gs_find_optimal_and_shortlist(women_prefenrence_lists, men_prefenrence_lists,
                                                        is_men_oriented=False)
        women_shortlists_t = copy.deepcopy(gs_woman_result["short_lists"])
        men_shortlists_t = copy.deepcopy(gs_woman_result["opposite_sex_short_lists"])
        Mt = copy.deepcopy(gs_woman_result["M"])

        n = len(men_shortlists_0)

        # merge shortlists
        men_shortlists, women_shortlists = merge_short_lists(men_shortlists_0, women_shortlists_0, men_shortlists_t,
                                                             women_shortlists_t)

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

        app = App(slaves=range(1, size))

        while True:
            # -------------------search forward---------------------
            if forward:
                tasks = []
                for h in range(len(neighbor_left)):
                    M_temp_man = copy.deepcopy(neighbor_left[h])
                    for m in range(n):
                        tasks.append((copy.deepcopy(men_shortlists),
                                      copy.deepcopy(women_shortlists),
                                      copy.deepcopy(M_temp_man),
                                      m,
                                      copy.deepcopy(Mt)))
                app.run(tasks)
                neigbor_set = copy.deepcopy(app.get_neigbor_set())
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
                        neighbor_left[0] = copy.deepcopy(neigbor_set[index_arr[0]])
                    else:
                        if k > len(neigbor_set):
                            neighbor_left = copy.deepcopy(neigbor_set)
                        else:
                            neighbor_left = neigbor_set[:k]
                else:
                    forward = False

            # -------------------search backward---------------------
            if backward:
                tasks = []
                for h in range(len(neighbor_right)):
                    M_temp_woman = copy.deepcopy(neighbor_right[h])
                    for m in range(n):
                        tasks.append((copy.deepcopy(women_shortlists),
                                      copy.deepcopy(men_shortlists),
                                      copy.deepcopy(M_temp_woman),
                                      m,
                                      copy.deepcopy(M0)))
                app.run(tasks)
                neigbor_set = copy.deepcopy(app.get_neigbor_set())
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
                        neighbor_right[0] = copy.deepcopy(neigbor_set[index_arr[0]])
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

        app.terminate_slaves()
        solution_normalize = [x + 1 for x in M_best]
        end = time.time()
        print(f'\n\nFound solution: {solution_normalize} in {end - start}\n\n')

    else:  # Any slave
        Worker().run()

    print(f'Task completed (rank {rank})')


def get_preference_lists(men_input: str, women_input: str):
    men_prefenrence_lists = utils.read_file(men_input)
    women_prefenrence_lists = utils.read_file(women_input)

    return men_prefenrence_lists, women_prefenrence_lists


def merge_short_lists(men_shortlists_0, women_shortlists_0, men_shortlists_t, women_shortlists_t):
    n = len(men_shortlists_0)
    men_shortlists = [[-1 for _ in range(n)] for _ in range(n)]
    women_shortlists = [[-1 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if men_shortlists_0[i][j] == men_shortlists_t[i][j]:
                men_shortlists[i][j] = men_shortlists_0[i][j]
            if women_shortlists_0[i][j] == women_shortlists_t[i][j]:
                women_shortlists[i][j] = women_shortlists_0[i][j]

    return men_shortlists, women_shortlists


if __name__ == "__main__":
    main()
