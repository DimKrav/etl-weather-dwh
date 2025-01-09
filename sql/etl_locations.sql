-- Create schema for ETL configuration
CREATE SCHEMA IF NOT EXISTS etl;

-- Create etl_locations table
CREATE TABLE IF NOT EXISTS etl.etl_locations (
    city_id SERIAL PRIMARY KEY,                
    city_name TEXT NOT NULL,              
    country TEXT NOT NULL,                
    latitude NUMERIC NOT NULL,            
    longitude NUMERIC NOT NULL,       
    active BOOLEAN DEFAULT TRUE,          
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
);

-- Insert initial data for locations
INSERT INTO etl.etl_locations (city_name, country, latitude, longitude) VALUES
    ('Moscow', 'Russia', 55.7558, 37.6173),
    ('New York', 'USA', 40.7128, -74.0060),
    ('Tokyo', 'Japan', 35.6895, 139.6917),
    ('Buenos Aires', 'Argentina', -34.6037, -58.3816),
    ('Cape Town', 'South Africa', -33.9249, 18.4241) 
    ON CONFLICT DO NOTHING;

-- Grant privileges
GRANT ALL PRIVILEGES ON TABLE etl.etl_locations TO public;
