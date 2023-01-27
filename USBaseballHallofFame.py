import pandas as pd
import mysql.connector as msql

from mysql.connector import Error

try:
    conn = msql.connect(host="localhost", 
                        user="sqluser",  
                        auth_plugin='mysql_native_password',
                        password="password",
                        database="lahman2016")
    if conn.is_connected():
        sql3 = "SELECT p.playerID AS Player, p.era as ERA, al.gp AS Game, h.yearid as 'Hallof Fame Induction Year' FROM Pitching p JOIN AllstarFull al ON p.playerID = al.playerID JOIN HallOfFame h ON h.playerID = p.playerID WHERE h.inducted = 'Y' and h.category = 'Player';"
        cols=['Player', 'ERA', 'Game', 'Year']
        pivotCol=['Player', 'Year']
        cursor = conn.cursor()
        cursor.execute(sql3)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=cols)
        meanf = df.groupby(pivotCol)['ERA'].mean()
        sumf = df.groupby(pivotCol)['Game'].sum()
        temp = pd.DataFrame(columns=['Player', 'ERA', '# All Star Appearances', 'Hall of Fame Induction Year'])
        temp['Player'] = meanf.index.get_level_values(0)
        temp['Hall of Fame Induction Year'] = meanf.index.get_level_values(1)
        temp['ERA'] = meanf.values
        temp['# All Star Appearances'] = sumf.values
        temp.to_csv("USBaseballHallofFame.csv", index=False)
except Error as e:
    print("Error while connecting to MySQL", e)