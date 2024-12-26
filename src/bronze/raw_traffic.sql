CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{tablename}`
(
  Record_ID INT,
  Count_point_id INT,
  Direction_of_travel VARCHAR(255),
  Year INT,
  Count_date VARCHAR(255),
  hour INT,
  Region_id INT,
  Region_name VARCHAR(255),
  Local_authority_name VARCHAR(255),
  Road_name VARCHAR(255),
  Road_Category_ID INT,
  Start_junction_road_name VARCHAR(255),
  End_junction_road_name VARCHAR(255),
  Latitude DOUBLE,
  Longitude DOUBLE,
  Link_length_km DOUBLE,
  Pedal_cycles INT,
  Two_wheeled_motor_vehicles INT,
  Cars_and_taxis INT,
  Buses_and_coaches INT,
  LGV_Type INT,
  HGV_Type INT,
  EV_Car INT,
  EV_Bike INT,
  Extract_Time TIMESTAMP
);