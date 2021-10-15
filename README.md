# PortfolioEvaluation

Use Mapreduce to filter and obtain return/covariance of historical data. Then proceed to form MVO/RMVO/ERC portfolios throuhg convex optimization. FX for a simple GUI. \\

Require HDFS (3.0+) and cvxpy (BLAS and LAPACK) properly configured. Change absolute pathing in config.json as needed. \\


Use prc or adjClose for CSV depending on avaliable data. HDFS input folder should contain a csv file in the format of [date, prc1, prc2,...]. Define ticker order in config.json['allowedTickers'].