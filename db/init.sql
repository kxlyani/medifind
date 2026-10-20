-- MediFind database schema + sample data
-- Runs automatically the first time the PostgreSQL container starts.

CREATE TABLE IF NOT EXISTS pharmacies (
    id    SERIAL PRIMARY KEY,
    name  VARCHAR(100) NOT NULL,
    area  VARCHAR(100) NOT NULL,
    lat   DOUBLE PRECISION NOT NULL,
    lng   DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS stock (
    pharmacy_id   INT REFERENCES pharmacies(id) ON DELETE CASCADE,
    medicine_name VARCHAR(100) NOT NULL,
    quantity      INT NOT NULL CHECK (quantity >= 0),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pharmacy_id, medicine_name)
);

CREATE INDEX IF NOT EXISTS idx_stock_medicine ON stock (LOWER(medicine_name));

-- One row per successful reservation ("hold this medicine for me")
CREATE TABLE IF NOT EXISTS reservations (
    id            SERIAL PRIMARY KEY,
    pharmacy_id   INT REFERENCES pharmacies(id) ON DELETE CASCADE,
    medicine_name VARCHAR(100) NOT NULL,
    quantity      INT NOT NULL CHECK (quantity > 0),
    customer      VARCHAR(50) NOT NULL DEFAULT 'guest',
    reserved_by   VARCHAR(100),          -- which Inventory pod handled it
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 20 sample pharmacies (coordinates are sample values; change them to your city if you like)
INSERT INTO pharmacies (name, area, lat, lng) VALUES
('Apollo Pharmacy',        'College Road',     18.5310, 73.8440),
('MedPlus',                'Station Road',     18.5285, 73.8740),
('Wellness Forever',       'MG Road',          18.5170, 73.8790),
('Shree Medical',          'Old City',         18.5180, 73.8550),
('Jan Aushadhi Kendra',    'Civil Hospital',   18.5250, 73.8620),
('Sai Medicals',           'Market Yard',      18.4890, 73.8640),
('Life Care Chemist',      'University Road',  18.5530, 73.8250),
('Om Medical Stores',      'Bus Stand',        18.5080, 73.8570),
('Ganesh Pharmacy',        'Industrial Area',  18.5680, 73.8970),
('City Chemist',           'Main Market',      18.5150, 73.8480),
('Health First Pharmacy',  'IT Park',          18.5920, 73.7380),
('Sanjivani Medical',      'Ring Road',        18.4700, 73.8300),
('Noble Chemist',          'Hospital Lane',    18.5020, 73.8240),
('Care & Cure Medicos',    'Railway Colony',   18.5370, 73.8890),
('Lifeline Pharmacy',      'New Town',         18.5590, 73.9100),
('Shiv Shakti Medical',    'Temple Street',    18.5000, 73.8620),
('Dhanvantari Medicals',   'Lake View',        18.4800, 73.8050),
('Om Sai Pharma',          'Airport Road',     18.5800, 73.9200),
('Prime Health Store',     'Tech Hub',         18.5500, 73.9400),
('Mahalaxmi Medical',      'Gandhi Nagar',     18.5450, 73.8000);

-- Give every pharmacy every medicine with a random quantity.
-- Roughly a third come out as 0 so search results look realistic.
SELECT setseed(0.42);
INSERT INTO stock (pharmacy_id, medicine_name, quantity)
SELECT p.id, m.name, GREATEST(0, FLOOR(random() * 60)::INT - 20)
FROM pharmacies p
CROSS JOIN (VALUES
    ('Paracetamol 500mg'), ('Dolo 650'), ('Crocin Advance'), ('Azithromycin 500mg'),
    ('Amoxicillin 500mg'), ('Cetirizine 10mg'), ('Montelukast 10mg'), ('Pantoprazole 40mg'),
    ('Omeprazole 20mg'), ('Metformin 500mg'), ('Glimepiride 2mg'), ('Insulin Glargine'),
    ('Amlodipine 5mg'), ('Telmisartan 40mg'), ('Atorvastatin 10mg'), ('Aspirin 75mg'),
    ('Ibuprofen 400mg'), ('Diclofenac Gel'), ('ORS Sachet'), ('Vitamin D3 60K'),
    ('Calcium + D3'), ('Iron Folic Acid'), ('Salbutamol Inhaler'), ('Levocetirizine 5mg'),
    ('Ondansetron 4mg'), ('Domperidone 10mg'), ('Cough Syrup (Benadryl)'), ('Betadine Ointment'),
    ('Thyroxine 50mcg'), ('Losartan 50mg')
) AS m(name);
