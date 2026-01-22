-- Create Databases
CREATE DATABASE publisher_db;
CREATE DATABASE distributor_db;
CREATE DATABASE player_db;

-- Create Users
CREATE USER publisher_user WITH PASSWORD 'publisher123';
CREATE USER distributor_user WITH PASSWORD 'distributor123';
CREATE USER player_user WITH PASSWORD 'player123';

-- Grant Owner Permissions (Read/Write)
ALTER DATABASE publisher_db OWNER TO publisher_user;
ALTER DATABASE distributor_db OWNER TO distributor_user;
ALTER DATABASE player_db OWNER TO player_user;

-- Grant Connect to everyone
GRANT CONNECT ON DATABASE publisher_db TO player_user, distributor_user;
GRANT CONNECT ON DATABASE distributor_db TO player_user, publisher_user;
GRANT CONNECT ON DATABASE player_db TO distributor_user, publisher_user;
