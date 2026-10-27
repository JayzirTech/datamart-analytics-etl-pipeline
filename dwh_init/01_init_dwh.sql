-- ==============================================================================
-- DATAMART S.A.S. - REPOSITORIO ANALÍTICO (DATA WAREHOUSE)
-- DDL DE INICIALIZACIÓN AUTOMÁTICA EN POSTGRESQL
-- Conforme a los Registros de Decisiones de Arquitectura (ADR-001 al ADR-0014)
-- ==============================================================================

-- 1. Dimensión de Divisas y Tasas de Cambio hacia USD (ADR-0014)
CREATE TABLE IF NOT EXISTS dim_exchange_rates (
    currency_code VARCHAR(10) PRIMARY KEY,
    currency_name VARCHAR(100) NOT NULL,
    country_default VARCHAR(100) NOT NULL,
    rate_to_usd NUMERIC(14, 6) NOT NULL,
    source VARCHAR(50) DEFAULT 'SEED_BACKUP',
    updated_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Semilla de respaldo para tasas de cambio iniciales (ADR-0014)
INSERT INTO dim_exchange_rates (currency_code, currency_name, country_default, rate_to_usd, source)
VALUES 
    ('GBP', 'British Pound Sterling', 'United Kingdom', 1.300000, 'SEED_BACKUP'),
    ('EUR', 'Euro', 'European Union', 1.080000, 'SEED_BACKUP'),
    ('USD', 'United States Dollar', 'USA', 1.000000, 'SEED_BACKUP'),
    ('COP', 'Colombian Peso', 'Colombia', 0.000250, 'SEED_BACKUP'),
    ('MXN', 'Mexican Peso', 'México', 0.055000, 'SEED_BACKUP'),
    ('PEN', 'Peruvian Sol', 'Perú', 0.270000, 'SEED_BACKUP'),
    ('AUD', 'Australian Dollar', 'Australia', 0.650000, 'SEED_BACKUP'),
    ('CAD', 'Canadian Dollar', 'Canada', 0.730000, 'SEED_BACKUP'),
    ('CHF', 'Swiss Franc', 'Switzerland', 1.140000, 'SEED_BACKUP'),
    ('JPY', 'Japanese Yen', 'Japan', 0.006700, 'SEED_BACKUP')
ON CONFLICT (currency_code) DO NOTHING;

-- 2. Dimensión de Clientes (Soporta clientes identificados y anónimos - ADR-002)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    customer_name VARCHAR(150) NOT NULL,
    customer_type VARCHAR(20) NOT NULL CHECK (customer_type IN ('IDENTIFICADO', 'ANONIMO')),
    country VARCHAR(100),
    first_seen_utc TIMESTAMP WITH TIME ZONE,
    created_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Inserción del registro canónico para transacciones sin cliente (ADR-002)
INSERT INTO dim_customers (customer_id, customer_name, customer_type, country)
VALUES ('ANONYMOUS', 'Consumidor Final / Anónimo', 'ANONIMO', 'No Especificado')
ON CONFLICT (customer_id) DO NOTHING;

-- 3. Dimensión de Productos (Enriquecida con API FastAPI y Moda de descripción - ADR-003, ADR-012)
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,
    stock_code VARCHAR(50) NOT NULL UNIQUE,
    canonical_description VARCHAR(255) NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    supplier_country VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Tabla de Hechos: Ventas Válidas (Cantidad > 0 y UnitPrice > 0, normalizadas a USD - ADR-008, ADR-014)
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_key BIGSERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    invoice_date DATE NOT NULL,
    invoice_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    product_key INT NOT NULL REFERENCES dim_products(product_key),
    customer_key INT NOT NULL REFERENCES dim_customers(customer_key),
    country VARCHAR(100) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    currency_original VARCHAR(10) NOT NULL DEFAULT 'GBP',
    unit_price_original NUMERIC(12, 4) NOT NULL CHECK (unit_price_original > 0),
    exchange_rate_to_usd NUMERIC(14, 6) NOT NULL DEFAULT 1.000000,
    unit_price NUMERIC(12, 4) NOT NULL CHECK (unit_price > 0), -- En USD
    gross_revenue NUMERIC(14, 4) NOT NULL CHECK (gross_revenue > 0), -- En USD (quantity * unit_price)
    source_file VARCHAR(50) NOT NULL,
    batch_id VARCHAR(100),
    loaded_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_fact_sales UNIQUE (invoice_no, product_key, invoice_timestamp_utc, quantity)
);

-- 5. Tabla de Hechos: Devoluciones / Cancelaciones (Cantidad <= 0 y UnitPrice > 0, normalizadas a USD)
CREATE TABLE IF NOT EXISTS fact_returns (
    return_key BIGSERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    invoice_date DATE NOT NULL,
    invoice_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    product_key INT NOT NULL REFERENCES dim_products(product_key),
    customer_key INT NOT NULL REFERENCES dim_customers(customer_key),
    country VARCHAR(100) NOT NULL,
    quantity_returned INT NOT NULL CHECK (quantity_returned > 0),
    currency_original VARCHAR(10) NOT NULL DEFAULT 'GBP',
    unit_price_original NUMERIC(12, 4) NOT NULL CHECK (unit_price_original > 0),
    exchange_rate_to_usd NUMERIC(14, 6) NOT NULL DEFAULT 1.000000,
    unit_price NUMERIC(12, 4) NOT NULL CHECK (unit_price > 0), -- En USD
    return_amount NUMERIC(14, 4) NOT NULL CHECK (return_amount > 0), -- En USD (quantity_returned * unit_price)
    source_file VARCHAR(50) NOT NULL,
    batch_id VARCHAR(100),
    loaded_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_fact_returns UNIQUE (invoice_no, product_key, invoice_timestamp_utc, quantity_returned)
);

-- 6. Tabla de Hechos Agregada: Revenue Diario por Producto en USD (Persistencia de Revenue Neto - ADR-008)
CREATE TABLE IF NOT EXISTS fact_daily_product_revenue (
    daily_summary_key BIGSERIAL PRIMARY KEY,
    invoice_date DATE NOT NULL,
    product_key INT NOT NULL REFERENCES dim_products(product_key),
    gross_revenue NUMERIC(14, 4) NOT NULL DEFAULT 0.0000, -- En USD
    return_amount NUMERIC(14, 4) NOT NULL DEFAULT 0.0000, -- En USD
    net_revenue NUMERIC(14, 4) NOT NULL DEFAULT 0.0000,   -- En USD (gross_revenue - return_amount)
    total_sold_qty INT NOT NULL DEFAULT 0,
    total_returned_qty INT NOT NULL DEFAULT 0,
    calculated_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_daily_product_revenue UNIQUE (invoice_date, product_key)
);

-- 7. Tabla de Log de Rechazos y Calidad de Datos (Auditoría exigida en sección 6 - ADR-009)
CREATE TABLE IF NOT EXISTS rejected_records (
    rejection_id BIGSERIAL PRIMARY KEY,
    source_file VARCHAR(100) NOT NULL,
    raw_record JSONB NOT NULL,
    rejection_reason VARCHAR(150) NOT NULL,
    batch_id VARCHAR(100),
    rejected_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==============================================================================
-- ÍNDICES DE RENDIMIENTO ANALÍTICO
-- ==============================================================================
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(invoice_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_country ON fact_sales(country);

CREATE INDEX IF NOT EXISTS idx_fact_returns_date ON fact_returns(invoice_date);
CREATE INDEX IF NOT EXISTS idx_fact_returns_product ON fact_returns(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_returns_customer ON fact_returns(customer_key);

CREATE INDEX IF NOT EXISTS idx_daily_revenue_date ON fact_daily_product_revenue(invoice_date);
CREATE INDEX IF NOT EXISTS idx_daily_revenue_product ON fact_daily_product_revenue(product_key);

CREATE INDEX IF NOT EXISTS idx_dim_products_code ON dim_products(stock_code);
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(category_name);
CREATE INDEX IF NOT EXISTS idx_dim_customers_id ON dim_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customers_type ON dim_customers(customer_type);

CREATE INDEX IF NOT EXISTS idx_rejected_reason ON rejected_records(rejection_reason);
