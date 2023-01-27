import pandas as pd
import mysql.connector as msql

from mysql.connector import Error

def getData(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=['Year', 'Player', 'Salary'])
    sf = df.groupby('Year')['Salary'].mean()
    return sf
try:
    conn = msql.connect(host="localhost", 
                        user="sqluser",  
                        auth_plugin='mysql_native_password',
                        password="password",
                        database="lahman2016")
    if conn.is_connected():
        sql = "SELECT f.yearid, f.playerID, s.salary FROM Fielding f JOIN Salaries s ON f.yearID = s.yearID and f.playerID = s.playerID"
        sql2 = "SELECT p.yearid , p.playerid, s.salary FROM Pitching p JOIN Salaries s ON p.yearID = s.yearID and p.playerID = s.playerID"
        ff = getData(conn, sql)
        pf = getData(conn, sql2)
        temp = pd.DataFrame(columns=['Year', 'Pitching','Fielding'])
        temp['Year'] = pf.index
        temp['Pitching'] = pf.values
        temp['Fielding'] = ff.values
        temp.to_csv("USBaseball.csv", index=False)
except Error as e:
    print("Error while connecting to MySQL", e)