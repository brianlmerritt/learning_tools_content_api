import pandas as pd
modules = pd.read_csv('course_data/RVC_BVETMED3_2024_5/RVC_BVETMED3_2024_5_modules.csv')

for _, module in modules.iterrows():
    print(module['module_name'])
