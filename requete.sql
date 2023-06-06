CREATE TABLE OF50(Horaire TIMESTAMP WITHOUT TIME ZONE,
                    id SERIAL PRIMARY KEY,
                    Latitude FLOAT, 
                    Longitude FLOAT, 
                    BSP FLOAT, 
                    SOG FLOAT, 
                    COG FLOAT, 
                    TWS FLOAT,
                    TWD FLOAT, 
                    TWA FLOAT,
                    Polaire FLOAT, 
                    Pression FLOAT,
                    V1 FLOAT,
                    V2 FLOAT,
                    V3 FLOAT,
                    V4 FLOAT, 
                    V5 FLOAT,
                    V6 FLOAT
);

INSERT INTO OF50(Horaire, Latitude, Longitude, BSP, SOG, COG, TWS, TWD, TWA, Polaire, Pression,V1,V2,V3,V4,V5,V6) values ('2022-09-28 20:41:00',49.488,0.093,0.09,0.09,340,5.4,193,205,80.0,0.0,0,0,0,0,0,0);