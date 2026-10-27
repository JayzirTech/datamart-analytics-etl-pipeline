-- ==============================================================================
-- DATAMART S.A.S. - REPOSITORIO ANALÍTICO (DATA WAREHOUSE)
-- DDL DE INICIALIZACIÓN AUTOMÁTICA EN POSTGRESQL
-- Conforme a los Registros de Decisiones de Arquitectura (ADR-001 al ADR-008)
-- ==============================================================================

-- 1. Dimensión de Clientes (Soporta clientes identificados y anónimos)
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

-- 2. Dimensión de Productos (Enriquecida con API FastAPI y Moda de descripción)
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

-- 3. Tabla de Hechos: Ventas Válidas (Cantidad > 0 y UnitPrice > 0)
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_key BIGSERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    invoice_date DATE NOT NULL,
    invoice_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    product_key INT NOT NULL REFERENCES dim_products(product_key),
    customer_key INT NOT NULL REFERENCES dim_customers(customer_key),
    country VARCHAR(100) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 4) NOT NULL CHECK (unit_price > 0),
    gross_revenue NUMERIC(14, 4) NOT NULL CHECK (gross_revenue > 0),
    source_file VARCHAR(50) NOT NULL,
    batch_id VARCHAR(100),
    loaded_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_fact_sales UNIQUE (invoice_no, product_key, invoice_timestamp_utc, quantity)
);

-- 4. Tabla de Hechos: Devoluciones / Cancelaciones (Cantidad <= 0 y UnitPrice > 0)
CREATE TABLE IF NOT EXISTS fact_returns (
    return_key BIGSERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    invoice_date DATE NOT NULL,
    invoice_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    product_key INT NOT NULL REFERENCES dim_products(product_key),
    customer_key INT NOT NULL REFERENCES dim_customers(customer_key),
    country VARCHAR(100) NOT NULL,
    quantity_returned INT NOT NULL CHECK (quantity_returned > 0),
    unit_price NUMERIC(12, 4) NOT NULL CHECK (unit_price > 0),
    return_amount NUMERIC(14, 4) NOT NULL CHECK (return_amount > 0),
    source_file VARCHAR(50) NOT NULL,
    batch_id VARCHAR(100),
    loaded_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_fact_returns UNIQUE (invoice_no, product_key, invoice_timestamp_utc, quantity_returned)
);

-- 5. Tabla de Hechos Agregada: Revenue Diario por Producto (Persistencia de Revenue Neto)
CREATE TABLE IF NOT EXISTS fact_daily_product_revenue (
    daily_summary_key BIGSERIAL PRIMARY KEY,
    invoice_date DATE NOT NULL,
    product_key INT NOT NULL REFERENCES dim_products(product_key),
    gross_revenue NUMERIC(14, 4) NOT NULL DEFAULT 0.0000,
    return_amount NUMERIC(14, 4) NOT NULL DEFAULT 0.0000,
    net_revenue NUMERIC(14, 4) NOT NULL DEFAULT 0.0000,
    total_sold_qty INT NOT NULL DEFAULT 0,
    total_returned_qty INT NOT NULL DEFAULT 0,
    calculated_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_daily_product_revenue UNIQUE (invoice_date, product_key)
);

-- 6. Tabla de Log de Rechazos y Calidad de Datos (Auditoría exigida en sección 6)
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
