--data warehouse
CREATE TABLE DimLocation (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(32) NOT NULL,
    country VARCHAR(32) NOT NULL,
    address VARCHAR(64) NOT NULL
);

CREATE TABLE DimAirport (
    airport_id SERIAL PRIMARY KEY,
    airport_name VARCHAR(32) NOT NULL,
    location_id INT,
    FOREIGN KEY (location_id) REFERENCES DimLocation(location_id)
);

CREATE TABLE DimPassenger (
    passenger_id SERIAL PRIMARY KEY,
    first_name VARCHAR(32) NOT NULL,
    last_name VARCHAR(32) NOT NULL,
    passport_details VARCHAR(32) NOT NULL
);

CREATE TABLE DimAirline (
    airline_id SERIAL PRIMARY KEY,
    airline_name VARCHAR(32) NOT NULL
);

CREATE TABLE DimGate (
    gate_id SERIAL PRIMARY KEY,
    airport_id INT NOT NULL,
    gate_name VARCHAR(32) NOT NULL,
    FOREIGN KEY (airport_id) REFERENCES DimAirport(airport_id)
);

CREATE TABLE DimFlightStatus (
    status_id SERIAL PRIMARY KEY,
    status_name VARCHAR(32) NOT NULL
);

CREATE TABLE DimDate (
    date_id SERIAL PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT,
    week_of_year INT
);

CREATE TABLE DimFlights (
    flight_id SERIAL PRIMARY KEY,
    gate_id INT NOT NULL,
    status_id INT NOT NULL,
    departure_time TIMESTAMP,
    arrival_time TIMESTAMP,
    arrival_airport INT NOT NULL, 
    flight_name VARCHAR(32) NOT NULL,
    airline_id INT NOT NULL,
    FOREIGN KEY (gate_id) REFERENCES DimGate(gate_id),
    FOREIGN KEY (status_id) REFERENCES DimFlightStatus(status_id),
    FOREIGN KEY (arrival_airport) REFERENCES DimAirport(airport_id), 
    FOREIGN KEY (airline_id) REFERENCES DimAirline(airline_id) 
);

CREATE TABLE DimTickets (
    ticket_id SERIAL PRIMARY KEY,
    flight_id INT NOT NULL,
    passenger_id INT NOT NULL,
    price DECIMAL NOT NULL,
    ticket_class VARCHAR(32) NOT NULL,
    FOREIGN KEY (flight_id) REFERENCES DimFlights(flight_id),
    FOREIGN KEY (passenger_id) REFERENCES DimPassenger(passenger_id)
);

CREATE TABLE DimBaggage (
    baggage_id SERIAL PRIMARY KEY,
    weight DECIMAL NOT NULL,
    description VARCHAR(255),
    ticket_id INT NOT NULL,
    FOREIGN KEY (ticket_id) REFERENCES DimTickets(ticket_id)
);

------ fact tables
CREATE TABLE FlightsFact (
    flight_id SERIAL PRIMARY KEY,
    gate_id INT NOT NULL,
    status_id INT NOT NULL,
    departure_date_id INT NOT NULL,
    arrival_airport_id INT NOT NULL,
    flight_name VARCHAR(32) NOT NULL,
    airline_id INT NOT NULL,
	PassengersCount INT ,
    BaggageWeight DECIMAL,
    FOREIGN KEY (gate_id) REFERENCES DimGate(gate_id),
    FOREIGN KEY (status_id) REFERENCES DimFlightStatus(status_id),
    FOREIGN KEY (departure_date_id) REFERENCES DimDate(date_id),
    FOREIGN KEY (arrival_airport_id) REFERENCES DimAirport(airport_id),
    FOREIGN KEY (airline_id) REFERENCES DimAirline(airline_id)
);

CREATE TABLE TicketsFact (
    ticket_id SERIAL PRIMARY KEY,
    flight_id INT NOT NULL,
    passenger_id INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    ticket_class VARCHAR(32) NOT NULL,
	ticket_count_by_class INT,
    FOREIGN KEY (flight_id) REFERENCES FlightsFact(flight_id),
    FOREIGN KEY (passenger_id) REFERENCES DimPassenger(passenger_id)
);

--SCD2
ALTER TABLE DimFlights
ADD COLUMN start_date TIMESTAMP;

ALTER TABLE DimFlights
ADD COLUMN end_date TIMESTAMP;

ALTER TABLE DimFlights
ADD COLUMN current_flag BOOLEAN DEFAULT TRUE;

ALTER TABLE DimFlights
DROP constraint FlightsFact_pkey

ALTER TABLE 
ADD COLUMN flightHistory_ID SERIAL PRIMARY KEY  ;

UPDATE DimFlights 
SET flightHistory_ID = DEFAULT;


UPDATE DimFlights
SET start_date = departure_time,
    end_date = '9999-12-31 ';


CREATE OR REPLACE FUNCTION DimFlights_update_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status_id <> NEW.status_id AND OLD.current_flag AND NEW.current_flag THEN
        UPDATE  DimFlights
        SET end_date = current_timestamp,
            current_flag = FALSE,
			status_id = OLD.status_id
        WHERE  flight_id  = OLD.flight_id  AND current_flag = TRUE;
		
        INSERT INTO  (flight_id,gate_id,status_id,departure_time,arrival_time,arrival_airport,flight_name,airline_id,start_date,end_date, current_flag)
        VALUES (OLD.flight_id, OLD.gate_id,NEW.status_id , NEW.departure_time, NEW.arrival_time, OLD.arrival_airport,OLD.flight_name,OLD.airline_id,current_timestamp, '9999-12-31', TRUE);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


