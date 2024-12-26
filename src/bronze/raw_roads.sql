CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{tablename}`
(
    Road_ID INT,
    Road_Category_Id INT,
    Road_Category VARCHAR(255),
    Region_ID INT,
    Region_Name VARCHAR(255),
    Total_Link_Length_Km DOUBLE,
    Total_Link_Length_Miles DOUBLE,
    All_Motor_Vehicles DOUBLE,
    Extract_Time TIMESTAMP
);
