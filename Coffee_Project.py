### Import data & libraries
import numpy as np
import pandas as pd
import seaborn as sns
import ipystata
import mysql.connector
from sqlalchemy import create_engine

# Establish connection
connection = mysql.connector.connect(
    host='localhost',       # e.g., 'localhost'
    user='root',   # e.g., 'root'
    password='Taichi!993',
    database='varhs'
)

query = "SELECT * FROM agri_production"
df = pd.read_sql(query, connection)
print(df.head())

# Close the connection
connection.close()

### Data cleaning
### Rename variables
df.rename(columns=
            {'p5ma_':'plot_code',
             'p15q1_':'crop_season_1',
             'p15q2_':'percent_area_season_1',
             'p15q3a_':'crop_quantity_season_1',
             'p15q3b_':'crop_unit_1',
             'p15q4_':'crop_reason_1',
             'p15q5_':'crop_season_2',
             'p15q6_':'percent_area_season_2',
             'p15q7a_':'crop_quantity_season_2',
             'p15q7b_':'crop_unit_2',
             'p15q8_':'crop_season_3',
             'p15q9_':'percent_area_season_3',
             'p15q10a_':'crop_quantity_season_3',
             'p15q10b_':'crop_unit_3',
             'p5q1_':'distance',
             'p5q3a_':'total_area',
             'p5q7_':'land_type',
             'p6q9_':'problems',
             'p6q10_':'plot_quality_village',
             'p6q10b_':'plot_quality_last_3_years',
             'p6q11_':'irrigation',
             'p6q12_':'water_source',
             'p7q15a_':'conservation_infrastructure_1',
             'p7q15b_':'conservation_infrastructure_2',
             'p8q1_':'manager_owned_plot',
             'p8q2a_':'red_book',
             'p8q2b_':'why_no_red_book',
             'p8q8_':'obtain_mortgage',
             'p8q9_':'could_obtain_mortgate',
             'p8q10_':'fallow_owned_plot',
             'p9q2_':'manager_rented_plot',
             'p9q8_':'fallow_rented_plot',
             'p12q1':'land_investment',
             'p12q2_a':'invest_type_1',
             'p12q2_b':'invest_type_2',
             'p13qa1_':'natural_disaster'},inplace=True)

#### Checking all variables for null and zero values
df_null=df.isnull().sum(axis=0)
df_zero=(df == 0).sum(axis=0)

#Create a new variable to identify plots that grew coffee as the main crop in 
#at least one season in 2016 (Coffee's code = 11)

df['Coffee_plot'] = 0

df.loc[(df.crop_season_1=='11') | 
       (df.crop_season_2=='11') | 
       (df.crop_season_3=='11'), 
       'Coffee_plot'] = 1

df['Coffee_plot'].value_counts()

# 5.9% of plots (468) grew coffee for at least one season in 2016

# Generate dataframe for coffee plots
df_cof=df[df.Coffee_plot==1]

# Provinces that has the most plots producing coffee in 2016: 
# Dak Lak (605) (38.9%), Dak Nong (606) (35%), Lam Dong (607) (23.5%)
df_cof.tinh_2016.value_counts(normalize=True)

# Summarize crop seasons
df_cof.crop_season_1.value_counts()
df_cof.crop_season_2.value_counts()
df_cof.crop_season_3.value_counts()

# Most coffee plots produced coffee in the most recent season (season 1),
# some plots produce coffee in season 2, not plot produces coffee in season 3
# Besides coffee, these plots also grow pepper, fruit, cashew nuts, rubber, ... 

df_cof[(df.crop_season_1==11) & (df.crop_season_2==11)]

# No plot produced coffee for more than one season

#### Cleaning coffee quantity
df_cof.dtypes
df_cof.crop_quantity_season_1=df_cof.crop_quantity_season_1.astype('float')

df_cof.loc[df_cof.crop_quantity_season_1.isin([98,99]),
           'crop_quantity_season_1'].value_counts()

df_cof.crop_quantity_season_2 = df_cof.crop_quantity_season_2.replace(r'^\s*$', np.nan, regex=True)
df_cof.crop_quantity_season_2=df_cof.crop_quantity_season_2.astype('float')

df_cof.loc[df_cof.crop_quantity_season_2.isin([98,99]),
           'crop_quantity_season_2'].value_counts()

df_cof.crop_quantity_season_3 = df_cof.crop_quantity_season_3.replace(r'^\s*$', np.nan, regex=True)
df_cof.loc[df_cof.crop_quantity_season_3.isin([98,99]),
           'crop_quantity_season_3'].value_counts()

# 7 coffee plots have not reaped yet in the most recent season, 
#so these plots did not have quantity data

# Replace Have not reaped yet & Don't know to null values
df_cof.crop_quantity_season_1.replace({98:np.nan, 
                                       99:np.nan},
                                      inplace=True)

# Because no plot produced coffee in season 3, 
# Coffee plots either produce coffee in season 1 or 2
df_cof['Coffee_quantity']=np.where(df_cof.crop_season_1=='11',
                                        df_cof.crop_quantity_season_1,
                                        np.where(df_cof.crop_season_2=='11',
                                        df_cof.crop_quantity_season_2,
                                        0))

# Summarize coffee quantity            
df_cof.Coffee_quantity.describe()                                       

sns.histplot(df_cof,
             x='Coffee_quantity', 
             bins=30, 
             kde=True, 
             color="orange", 
             edgecolor="black")

# Summarize the percentage of area each plot use to product coffee
df_cof['Coffee_area_percent']=np.where(df_cof.crop_season_1=='11',
                                        df_cof.percent_area_season_1,
                                        np.where(df_cof.crop_season_2=='11',
                                        df_cof.percent_area_season_2,
                                        0))

df_cof.Coffee_area_percent=df_cof.Coffee_area_percent.astype('float')
df_cof.Coffee_area_percent.describe()                                       

sns.histplot(df_cof,
             x='Coffee_area_percent', 
             bins=30, 
             kde=True, 
             color="orange", 
             edgecolor="black")

# Most coffee plot use 100% area to grow coffee

# Turn percent area into real area
df_cof.total_area.describe()

df_cof['Coffee_area']=df_cof.Coffee_area_percent/100 * df_cof.total_area

df_cof[['total_area','Coffee_area_percent','Coffee_area']].describe()

sns.histplot(df_cof,
             x='Coffee_area', 
             bins=30, 
             kde=True, 
             color="orange", 
             edgecolor="black")                                      

# Convert coffee quantity from kg to tonnes
df_cof['Coffee_quantity_tonnes']=df_cof.Coffee_quantity/1000

# Convert coffee area from sqm to hectare
df_cof['Coffee_area_hectare']=df_cof.Coffee_area/10000

# Generate coffee productivity variable
df_cof['Coffee_productivity']=df_cof.Coffee_quantity_tonnes/df_cof.Coffee_area_hectare

df_cof[['Coffee_quantity_tonnes','Coffee_area_hectare','Coffee_productivity']].describe()                                       

sns.histplot(df_cof,
             x='Coffee_productivity', 
             bins=30, 
             kde=True, 
             color="orange", 
             edgecolor="black")

df_cof[['Coffee_productivity','Coffee_area_hectare']].corr()

# Coffee productivity has a negative relationship with coffee area,
# which means small area often has higher productivity than large area
# we also observe that productivity outliers often come from these plots

# Crop reasons: Traditional vs High productivity

df_cof.crop_reason_1.value_counts() 

# Most popular reasons of growing coffee are "High productivity/high efficiency" (code 2)
# And "Traditional crop" (Code 5)                                     

sum=df_cof[df_cof.crop_season_1==11].groupby('crop_reason_1')[['Coffee_productivity',
                                     'Coffee_area_hectare',
                                     'Coffee_quantity_tonnes']].agg(['mean','count'])

df_cof[df_cof.crop_reason_1==2]['tinh_2016'].value_counts()
df_cof[df_cof.crop_reason_1==5]['tinh_2016'].value_counts()

  # Dak Lak (code 606) has higher number of 'traditional' plots while 
# Dak Nong (code 605) has higher number of 'high productivity' plots
# Can use spatial map to visualize this

# 'Traditional crop' plots have higher productivity than 'High productivity' plots
# However, 'High productivity' plots have higher quantity than 'Traditional crop' plots
# Mostly because 'High productivity' plots are larger than 'Traditional crop' plots

# Problems: Dry land
df_cof.problems=df_cof.problems.replace('\\N',np.nan)
df_cof.problems=df_cof.problems.astype('float')
df_cof.problems.dtypes
df_cof.problems.value_counts()
df_cof[df_cof.problems=='2']['tinh_2016'].value_counts()
df_cof[df_cof.tinh_2016==605]['problems'].value_counts(normalize=True)

# Most popular problem for coffee plots is dry land, focus in Dak Nong
# Dak Nong (605) has a dry land problem, more than 60% of coffee plots in Dak Nong
# has dry land problem

# How the dry land problem affects coffee quantity and productivity?
sum=df_cof.groupby('problems')[['Coffee_productivity',
                                     'Coffee_area_hectare',
                                     'Coffee_area_percent',
                                     'Coffee_quantity_tonnes']].agg(['mean','count'])

# Dry land plots have lower quantity and area but higher productivity than 'No problem' plots



# Most coffee is irrigated and grown in perennial crops land or houses with 
# garden with distance to home less than 3 km. Quality of most plot is equal to the average quality of the village.
# However, 20% of the plots has decreasing quality over the last
# three years. Most popular water sources are dug wells, springs, rivers, ponds, lakes
# Most plots do no have any soil or water conservation infrastructures
# Only 10% (50 plots) has some conservation infrastructure
df_cof.land_type.value_counts()
df_cof.plot_quality_village.value_counts()
df_cof.plot_quality_last_3_years.value_counts(normalize=True)
df_cof.irrigation.value_counts()
df_cof.water_source.value_counts()
df_cof.distance=df_cof.distance.replace('\\N',np.nan)
df_cof.distance=df_cof.distance.astype('float')
df_cof.distance.describe()
df_cof.conservation_infrastructure_1.value_counts()

# Identify coffee plots that conduct intercropping: 37 plots
df_cof['intercropping']=np.where((~df_cof.crop_season_1.isin([11,23,24,np.nan])) |
                                 (~df_cof.crop_season_2.isin([11,23,24,np.nan])) |
                                 (~df_cof.crop_season_3.isin([11,23,24,np.nan])), 
                                  1, 0)

df_cof.intercropping.value_counts(normalize=True)

df_cof.red_book.describe()
df_cof.red_book.value_counts()
df_cof.red_book=df_cof.red_book.replace('\\N',np.nan)
df_cof.red_book=df_cof.red_book.astype('float')
df_cof.red_book.value_counts(dropna=False)
df_cof.manager_owned_plot.value_counts() 
df_cof.manager_owned_plot=df_cof.manager_owned_plot.replace('\\N',np.nan)

# Create SQLAlchemy engine
engine = create_engine("mysql+mysqlconnector://root:Taichi!993@localhost/varhs")

# Write DataFrame to MySQL
df_cof.to_sql('coffee_production', con=engine, if_exists='replace', index=False)
print("Data loaded successfully!")
engine.dispose()  # Releases all connections in the connection pool
