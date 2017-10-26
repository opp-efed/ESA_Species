import pandas as pd

in_range = pd.read_csv(
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\R_Acres_by_region_20170131_yes_no.csv')
regions = ['AK', 'AS', 'CNMI', 'GU', 'HI', 'Howland Baker Jarvis', 'Johnston', 'CONUS', 'Laysan', 'Mona', 'Necker',
           'Nihoa', 'NorthwesternHI', 'PR', 'Palmyra Kingman', 'VI', 'Wake']


for v in regions:
    in_range[v] = in_range[v].apply(lambda x: 'Yes' if x <= 1 else 'nan')
