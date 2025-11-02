-- Table: public.tbl_motor_theft_vehicles

DROP TABLE IF EXISTS public.tbl_motor_theft_vehicles;

CREATE TABLE IF NOT EXISTS public.tbl_motor_theft_vehicles
(
    incident_id VARCHAR,
    report_number VARCHAR,
    report_datetime VARCHAR,
    occurrence_datetime VARCHAR,
    addressline VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zipcode VARCHAR,
    vehicle_id VARCHAR,
    vehicle_year VARCHAR,
    vehicle_color VARCHAR,
    licenseplate VARCHAR,
    vin VARCHAR,
    recovery_status VARCHAR,
    recovery_date VARCHAR,
    borough VARCHAR,
    data_source VARCHAR,
    incident_status VARCHAR,
    method_of_entry VARCHAR
);
