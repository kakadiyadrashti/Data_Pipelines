{% snapshot dim_lga %}
    {{
        config(
            target_schema='exactly',
            unique_key='listing_neighbourhood',
            strategy='timestamp',
            updated_at='scraped_date'
        )
    }}

    SELECT
        DISTINCT
        listing_neighbourhood,
        host_neighbourhood,
        property_type,
        price
    FROM exactly.silver_listings

{% endsnapshot %}
