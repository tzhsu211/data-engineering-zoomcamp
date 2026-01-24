import os
import sys
import pandas as pd

print("arg: ", sys.argv)
month = int(sys.argv[1])
df = pd.DataFrame({"days": [1,2], "num_passengers": [100,200]})
df['month'] = month
print(df.head())
print(f"Hello pipeline. month = {month}")
