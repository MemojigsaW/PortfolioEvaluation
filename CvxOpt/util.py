import numpy as np

FILENAME = "part-r-00000"


def loadRet(path: str, dim):
    """
    format of int \t double
    :param path:
    :param dim:
    :return:
    """
    result = np.empty((dim,), dtype=np.float64)

    path += "p2" + "/" + FILENAME
    f = open(path, "r")
    lines = f.readlines()
    f.close()

    for line in lines:
        parse = line.split("\t")
        index = int(parse[0])
        val = float(parse[1])
        result[index] = val
    return result


def loadCov(path: str, dim):
    """
    format of int,int \t double
    :param path:
    :return:
    """
    result = np.empty((dim, dim), dtype=np.float64)
    path += "/" + FILENAME
    f = open(path, "r")
    lines = f.readlines()
    f.close()

    for line in lines:
        parse = line.split("\t")
        parseIndex = parse[0].split(",")
        r = int(parseIndex[0])
        c = int(parseIndex[1])
        val = float(parse[1])
        result[r][c] = val
        if r != c:
            result[c][r] = val

    return result
