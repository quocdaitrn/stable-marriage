def read_file(file_name: str) -> list:
    with open(file_name, 'r') as f:
        f_skip_fist_line = f.readlines()[1:]
        matrix = [[(int(num) - 1) for num in line.split(' ')] for line in f_skip_fist_line if line.strip() != ""]
    return matrix


def matching_cost(men_short_lists: list, women_short_list: list, M: list):
    n = len(M)
    sm = 0
    sw = 0
    for i in range(n):
        mi = i
        wi = M[i]
        mr = find(men_short_lists[mi], wi)
        wr = find(women_short_list[wi], mi)
        sm = sm + mr + 1
        sw = sw + wr + 1

    # egalitarian cost
    # fm = sm + sw;

    # sex - equal cost
    fm = abs(sm - sw)

    return fm, sm, sw


def find(l: list, val: int) -> int:
    try:
        return l.index(val)
    except ValueError:
        return -1

