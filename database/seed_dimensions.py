"""
Seeds reference dimension tables in PostgreSQL (or local fallback)
with realistic master data for customers, products, locations, payment methods, and dates.
"""
import random
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import text
from database.db_connection import db_manager
from config.logging_config import setup_logger

logger = setup_logger("db_seeder")
fake = Faker()
Faker.seed(42)
random.seed(42)

# Predefined realistic product catalogs
PRODUCT_CATALOG = [
    # Electronics
    ("PROD-E101", "MacBook Pro 16 M3 Max", "Electronics", "Laptops", 3499.00, 2600.00, "SUPP-APPLE"),
    ("PROD-E102", "Dell XPS 15 OLED", "Electronics", "Laptops", 1999.00, 1450.00, "SUPP-DELL"),
    ("PROD-E103", "Sony WH-1000XM5 Headphones", "Electronics", "Audio", 399.99, 240.00, "SUPP-SONY"),
    ("PROD-E104", "Apple iPhone 15 Pro Max 256GB", "Electronics", "Smartphones", 1199.00, 850.00, "SUPP-APPLE"),
    ("PROD-E105", "Samsung Galaxy S24 Ultra", "Electronics", "Smartphones", 1299.00, 920.00, "SUPP-SAMSUNG"),
    ("PROD-E106", "LG C3 65-inch 4K OLED TV", "Electronics", "Television", 1799.99, 1250.00, "SUPP-LG"),
    ("PROD-E107", "Logitech MX Master 3S Wireless Mouse", "Electronics", "Accessories", 99.99, 55.00, "SUPP-LOGI"),
    ("PROD-E108", "Keychron Q1 Pro Mechanical Keyboard", "Electronics", "Accessories", 199.00, 110.00, "SUPP-KEYCHRON"),
    
    # Apparel
    ("PROD-A201", "Patagonia Better Sweater Fleece", "Apparel", "Outerwear", 159.00, 65.00, "SUPP-PATAGONIA"),
    ("PROD-A202", "Nike Air Zoom Pegasus 40", "Apparel", "Footwear", 130.00, 52.00, "SUPP-NIKE"),
    ("PROD-A203", "Levi's 501 Original Fit Jeans", "Apparel", "Pants", 79.50, 30.00, "SUPP-LEVIS"),
    ("PROD-A204", "Lululemon Align High-Rise Pant", "Apparel", "Activewear", 98.00, 32.00, "SUPP-LULU"),
    ("PROD-A205", "Arc'teryx Beta AR Gore-Tex Jacket", "Apparel", "Outerwear", 600.00, 290.00, "SUPP-ARCTERYX"),
    ("PROD-A206", "Ray-Ban Classic Aviator Polarized", "Apparel", "Accessories", 221.00, 75.00, "SUPP-LUXOTTICA"),
    
    # Home & Kitchen
    ("PROD-H301", "Breville Barista Touch Espresso Machine", "Home & Kitchen", "Appliances", 999.95, 620.00, "SUPP-BREVILLE"),
    ("PROD-H302", "Dyson V15 Detect Cordless Vacuum", "Home & Kitchen", "Appliances", 749.99, 450.00, "SUPP-DYSON"),
    ("PROD-H303", "Le Creuset Enameled Cast Iron Dutch Oven", "Home & Kitchen", "Cookware", 420.00, 180.00, "SUPP-LECREUSET"),
    ("PROD-H304", "KitchenAid Artisan Stand Mixer 5-Qt", "Home & Kitchen", "Appliances", 449.99, 260.00, "SUPP-WHIRLPOOL"),
    ("PROD-H305", "Ninja Foodi 8-in-1 DualZone Air Fryer", "Home & Kitchen", "Appliances", 199.99, 105.00, "SUPP-SHARKNINJA"),
    
    # Sports & Outdoors
    ("PROD-S401", "Yeti Tundra 45 Hard Cooler", "Sports & Outdoors", "Camping", 325.00, 160.00, "SUPP-YETI"),
    ("PROD-S402", "Garmin Fenix 7 Pro Sapphire Solar Watch", "Sports & Outdoors", "Fitness GPS", 799.99, 480.00, "SUPP-GARMIN"),
    ("PROD-S403", "Hydro Flask 32 oz Wide Mouth Bottle", "Sports & Outdoors", "Hydration", 44.95, 18.00, "SUPP-HYDROFLASK"),
    ("PROD-S404", "Bowflex SelectTech 552 Adjustable Dumbbells", "Sports & Outdoors", "Fitness", 429.00, 240.00, "SUPP-BOWFLEX"),
    
    # Beauty & Personal Care
    ("PROD-B501", "Dyson Supersonic Hair Dryer", "Beauty & Care", "Hair Care", 429.99, 260.00, "SUPP-DYSON"),
    ("PROD-B502", "La Mer Crème de la Mer Moisturizer 60ml", "Beauty & Care", "Skincare", 380.00, 120.00, "SUPP-ESTEE"),
    ("PROD-B503", "Oral-B iO Series 9 Electric Toothbrush", "Beauty & Care", "Oral Care", 299.99, 140.00, "SUPP-PG"),
]

# Predefined locations
LOCATIONS = [
    ("LOC-NY01", "New York", "NY", "USA", "10001", "North America", 40.7128, -74.0060),
    ("LOC-CA01", "San Francisco", "CA", "USA", "94102", "North America", 37.7749, -122.4194),
    ("LOC-WA01", "Seattle", "WA", "USA", "98101", "North America", 47.6062, -122.3321),
    ("LOC-TX01", "Austin", "TX", "USA", "73301", "North America", 30.2672, -97.7431),
    ("LOC-IL01", "Chicago", "IL", "USA", "60601", "North America", 41.8781, -87.6298),
    ("LOC-UK01", "London", "Greater London", "UK", "EC1A 1BB", "EMEA", 51.5074, -0.1278),
    ("LOC-DE01", "Berlin", "Berlin", "Germany", "10115", "EMEA", 52.5200, 13.4050),
    ("LOC-FR01", "Paris", "Île-de-France", "France", "75001", "EMEA", 48.8566, 2.3522),
    ("LOC-JP01", "Tokyo", "Tokyo", "Japan", "100-0001", "APAC", 35.6762, 139.6503),
    ("LOC-SG01", "Singapore", "Central", "Singapore", "018989", "APAC", 1.3521, 103.8198),
    ("LOC-AU01", "Sydney", "NSW", "Australia", "2000", "APAC", -33.8688, 151.2093),
    ("LOC-IN01", "Bengaluru", "Karnataka", "India", "560001", "APAC", 12.9716, 77.5946),
    ("LOC-BR01", "São Paulo", "SP", "Brazil", "01310-100", "LATAM", -23.5505, -46.6333),
    ("LOC-CA-TOR", "Toronto", "ON", "Canada", "M5H 2N2", "North America", 43.6532, -79.3832),
]

# Predefined payment methods
PAYMENT_METHODS = [
    ("PAY-CC-VISA", "Credit Card", "Visa", True),
    ("PAY-CC-MC", "Credit Card", "Mastercard", True),
    ("PAY-CC-AMEX", "Credit Card", "American Express", True),
    ("PAY-DC-VISA", "Debit Card", "Visa Debit", True),
    ("PAY-PAYPAL", "Digital Wallet", "PayPal", True),
    ("PAY-APPLE", "Digital Wallet", "Apple Pay", True),
    ("PAY-GOOGLE", "Digital Wallet", "Google Pay", True),
    ("PAY-KLARNA", "BNPL", "Klarna", True),
    ("PAY-CRYPTO", "Cryptocurrency", "BitPay", True),
]


def seed_all_dimensions():
    """Populates all dimension tables with realistic master data."""
    logger.info("Starting dimension table seeding...")

    with db_manager.engine.begin() as conn:
        # 1. Seed Payment Methods
        for p_id, p_type, provider, active in PAYMENT_METHODS:
            conn.execute(
                text("""
                INSERT INTO dim_payment_methods (payment_method_id, method_type, provider, is_active)
                VALUES (:id, :type, :provider, :active)
                ON CONFLICT (payment_method_id) DO UPDATE 
                SET method_type = EXCLUDED.method_type, provider = EXCLUDED.provider
                """),
                {"id": p_id, "type": p_type, "provider": provider, "active": 1 if active else 0}
            )
        logger.info(f"Seeded {len(PAYMENT_METHODS)} payment methods.")

        # 2. Seed Locations
        for loc in LOCATIONS:
            conn.execute(
                text("""
                INSERT INTO dim_locations (location_id, city, state, country, postal_code, region, latitude, longitude)
                VALUES (:id, :city, :state, :country, :postal, :region, :lat, :lon)
                ON CONFLICT (location_id) DO UPDATE 
                SET city = EXCLUDED.city, state = EXCLUDED.state, country = EXCLUDED.country
                """),
                {
                    "id": loc[0], "city": loc[1], "state": loc[2], "country": loc[3],
                    "postal": loc[4], "region": loc[5], "lat": loc[6], "lon": loc[7]
                }
            )
        logger.info(f"Seeded {len(LOCATIONS)} locations.")

        # 3. Seed Products
        for prod in PRODUCT_CATALOG:
            conn.execute(
                text("""
                INSERT INTO dim_products (product_id, product_name, category, subcategory, base_price, cost_price, supplier_id, inventory_count)
                VALUES (:id, :name, :cat, :subcat, :base, :cost, :supp, :inv)
                ON CONFLICT (product_id) DO UPDATE 
                SET base_price = EXCLUDED.base_price, cost_price = EXCLUDED.cost_price, inventory_count = EXCLUDED.inventory_count
                """),
                {
                    "id": prod[0], "name": prod[1], "cat": prod[2], "subcat": prod[3],
                    "base": prod[4], "cost": prod[5], "supp": prod[6], "inv": random.randint(500, 3000)
                }
            )
        logger.info(f"Seeded {len(PRODUCT_CATALOG)} products.")

        # 4. Seed Customers (150 realistic profiles)
        customer_count = 150
        segments = ["VIP", "Enterprise", "Regular", "Standard"]
        segment_weights = [0.10, 0.15, 0.45, 0.30]

        for i in range(1, customer_count + 1):
            cust_id = f"CUST-{1000 + i}"
            name = fake.name()
            email = f"{name.lower().replace(' ', '.')}_{i}@{fake.free_email_domain()}"
            segment = random.choices(segments, weights=segment_weights)[0]
            city = random.choice(LOCATIONS)[1]
            country = "USA" if random.random() > 0.3 else random.choice(LOCATIONS)[3]
            signup = datetime.now() - timedelta(days=random.randint(10, 730))

            conn.execute(
                text("""
                INSERT INTO dim_customers (customer_id, customer_name, email, segment, city, country, signup_date, is_active)
                VALUES (:id, :name, :email, :segment, :city, :country, :signup, :active)
                ON CONFLICT (customer_id) DO UPDATE 
                SET segment = EXCLUDED.segment, email = EXCLUDED.email
                """),
                {
                    "id": cust_id, "name": name, "email": email, "segment": segment,
                    "city": city, "country": country, "signup": signup, "active": 1
                }
            )
        logger.info(f"Seeded {customer_count} customers.")

        # 5. Seed Date Dimension (Past 30 days + Future 30 days)
        start_date = datetime.now() - timedelta(days=30)
        for d in range(60):
            cur_date = (start_date + timedelta(days=d)).date()
            date_key = int(cur_date.strftime("%Y%m%d"))
            conn.execute(
                text("""
                INSERT INTO dim_dates (date_key, full_date, day_of_week, day_name, month_num, month_name, quarter, year, is_weekend)
                VALUES (:key, :full, :dow, :dname, :mnum, :mname, :qtr, :yr, :weekend)
                ON CONFLICT (date_key) DO NOTHING
                """),
                {
                    "key": date_key,
                    "full": cur_date,
                    "dow": cur_date.isoweekday(),
                    "dname": cur_date.strftime("%A"),
                    "mnum": cur_date.month,
                    "mname": cur_date.strftime("%B"),
                    "qtr": (cur_date.month - 1) // 3 + 1,
                    "yr": cur_date.year,
                    "weekend": 1 if cur_date.isoweekday() in (6, 7) else 0
                }
            )
        logger.info("Seeded 60 calendar date dimension rows.")

    logger.info("Dimension table seeding completed successfully!")


if __name__ == "__main__":
    seed_all_dimensions()
