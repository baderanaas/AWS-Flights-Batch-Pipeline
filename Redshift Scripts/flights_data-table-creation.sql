CREATE TABLE flights_data (
    flight_status VARCHAR(255),
    departure_airport_name VARCHAR(255),
    departure_airport_icao VARCHAR(50),
    airport_timezone VARCHAR(255),
    departure_terminal VARCHAR(50),
    departure_gate VARCHAR(50),
    departure_delay INT,
    scheduled_departure_time VARCHAR(255),
    actual_departure_time VARCHAR(255),
    arrival_airport_name VARCHAR(255),
    arrival_airport_icao VARCHAR(50),
    arrival_timezone VARCHAR(255),
    airline_name VARCHAR(255),
    airline_icao VARCHAR(50),
    flight_icao VARCHAR(50),
    aircraft_icao24 VARCHAR(50),
    flight_date VARCHAR(255)
);
