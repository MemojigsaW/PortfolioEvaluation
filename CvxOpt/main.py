import json
import sys
import util
import pandas as pd
from datetime import datetime
import warnings
from CvxOPT import port_module as pt


warnings.filterwarnings("ignore")

f = open(sys.argv[1], "r")
config = json.load(f)
f.close()
selectedTickers = sys.argv[12:]

start_date = datetime.strptime(sys.argv[2], "%m/%d/%Y")
mid_date = datetime.strptime(sys.argv[3], "%m/%d/%Y")
end_date = datetime.strptime(sys.argv[4], "%m/%d/%Y")
# type
mode = sys.argv[5]
# r_lambda
r_lambda = float(sys.argv[6])
# short
if sys.argv[7]=="True":
    short = True
else:
    short = False
# LN dim
# control if (-1)
LN = int(sys.argv[8])
# LN bound
LN_bound = float(sys.argv[9])
# RMVO type
RMVO_type = sys.argv[10]
# RMVO conf interval
conf_int = float(sys.argv[11])

output_name = config["hdfs_output"].split("/")[-1]
dim = len(selectedTickers)
n_sample = (mid_date.year - start_date.year) * 12 + (mid_date.month - start_date.month)

ret = util.loadRet(config["file_Loc"] + "/" + output_name, dim)
cov = util.loadCov(config["file_Loc"] + "/" + output_name, dim)

xv = None
try:
    if mode == "MVO":
        if LN < 0:
            LN = None
        r, v, xv = pt.MVO(ret, cov, r_lambda=r_lambda, short=short, LN=LN, LN_bound=LN_bound)
    elif mode == "RMVO":
        r, v, xv = pt.RMVO(ret, cov, n_sample, r_lambda=r_lambda, short=short, option=RMVO_type, con_lvl=conf_int)
    elif mode == "ERC":
        r, v, xv = pt.ERC(ret, cov)

    if xv is None:
        sys.exit(1)


    test_folder = config["hdfs_output"].split("/")[-1] + "test"
    df = pd.read_csv(config["file_Loc"] + "/" + test_folder + "/" + "part-r-00000", parse_dates=[0],
                     header=selectedTickers.insert(0, "date"), index_col=[0])

    print(', '.join(map("{:.6f}".format, xv)))

    df.sort_index(inplace=True)

    start_port_value = df.iloc[0, :] @ xv

    df = df.dot(xv) / start_port_value

    df.to_csv(config["file_Loc"] + "/testplot.csv", header=False)

except Exception as e:
    print(e)
    sys.exit(1)

# "json_pth", start, mid, end, model_type, r_lambda, short, LN, LN_bound, RMVO_type, [list of tickers]
