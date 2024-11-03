DROP TABLE IF EXISTS cities, airports, flights, flight_prices, booking_places, accommodations,accommodation_prices, activities, activity_prices, activity_availabilities  CASCADE;


-- Create Cities table
CREATE TABLE cities (
    city_entityid SERIAL PRIMARY KEY,
    city_name VARCHAR(255) NOT NULL,
    country VARCHAR(100)
);

-- Create airports table
CREATE TABLE airports (
    airport_entityid SERIAL PRIMARY KEY,
    airport_skyid VARCHAR(10) NOT NULL,
    airport_name VARCHAR(255) NOT NULL,
    city_entityid INT REFERENCES cities(city_entityid) ON DELETE SET NULL
);


-- Create flights table
CREATE TABLE flights (
    itinerary_id VARCHAR(255) PRIMARY KEY,
    origin_airport_entityid INT REFERENCES airports(airport_entityid),
    destination_airport_entityid INT REFERENCES airports(airport_entityid),
    departure_datetime TIMESTAMP NOT NULL,
    arrival_datetime TIMESTAMP NOT NULL,
    company VARCHAR(100),
    self_transfer BOOLEAN,
    fare_is_change_allowed BOOLEAN,
    fare_is_partially_changeable BOOLEAN,
	fare_is_cancellation_allowed BOOLEAN,
	fare_is_partially_refundable BOOLEAN
);

-- Create flight_prices table
CREATE TABLE flight_prices (
    price_id SERIAL PRIMARY KEY,
    itinerary_id VARCHAR(255) REFERENCES flights(itinerary_id),
    query_date TIMESTAMP NOT NULL,
    price NUMERIC NOT NULL,
    price_currency VARCHAR(10) NOT NULL,
    score NUMERIC
);


-- Create Booking places table
CREATE TABLE booking_places (
    place_id SERIAL PRIMARY KEY,
    city_entityid INT REFERENCES cities(city_entityid) ON DELETE SET NULL, 
    name VARCHAR(255) NOT NULL,
    url VARCHAR(255),
    distance_city_center_km NUMERIC,
    score NUMERIC,
    n_comments INT,
    close_to_metro BOOLEAN,
    sustainability_cert BOOLEAN,
    location_score NUMERIC
);

-- Create Accommodations table
CREATE TABLE accommodations (
    accommodation_id SERIAL PRIMARY KEY,
    place_id INT REFERENCES booking_places(place_id),
    room_type VARCHAR(100),
    standardized_room_type VARCHAR(100),
    double_bed INT DEFAULT 0,
    single_bed INT DEFAULT 0,
    shared_bathroom BOOLEAN,
    balcony BOOLEAN
);

-- Create Prices table
CREATE TABLE accommodation_prices (
    price_id SERIAL PRIMARY KEY,
    accommodation_id INT REFERENCES accommodations(accommodation_id),
    query_date TIMESTAMP NOT NULL,
    checkin DATE NOT NULL,
    checkout DATE NOT NULL,
    n_adults INT NOT NULL,
    n_children INT DEFAULT 0,
    n_rooms INT NOT NULL,
    price_night NUMERIC NOT NULL,
    price_currency VARCHAR(4) NOT NULL,
    free_cancellation BOOLEAN,
    pay_at_hotel BOOLEAN,
    free_taxi BOOLEAN
);


-- Create Activities table
CREATE TABLE activities (
    activity_id SERIAL PRIMARY KEY,
    activity_name VARCHAR(255) NOT NULL,
    city_entityid INT REFERENCES cities(city_entityid) ON DELETE SET NULL,  -- Link to Cities table for normalization
    description TEXT,
    url VARCHAR(500),
    image VARCHAR(255),
    duration VARCHAR(30),  -- Stores time duration in hours, minutes, etc.
    latitude NUMERIC,
    longitude NUMERIC,
    category VARCHAR(100),
    spanish VARCHAR(30),
    address VARCHAR(255)
);

-- Create Activity Prices table
CREATE TABLE activity_prices (
    price_id SERIAL PRIMARY KEY,
    activity_id INT REFERENCES activities(activity_id) ON DELETE CASCADE,
    query_date DATE NOT NULL,  -- Stores date of the price query
    price NUMERIC NOT NULL,
    currency VARCHAR(4) NOT NULL  -- Uses ISO currency code
);

-- Create Activity Availabilities table
CREATE TABLE activity_availabilities (
    schedule_id SERIAL PRIMARY KEY,
    activity_id INT REFERENCES activities(activity_id) ON DELETE CASCADE,
    query_date TIMESTAMP NOT NULL,  -- Stores date when availability was queried
    available_date DATE NOT NULL,  -- Date when the activity is available
    available_time TIME NOT NULL  -- Time when the activity is available
);


