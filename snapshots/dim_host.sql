{% snapshot dim_host %}
    {{
        config(
            target_schema='exactly',
            unique_key='host_id',
            strategy='timestamp',
            updated_at='host_since'
        )
    }}

    SELECT
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood
    FROM exactly.silver_listings

{% endsnapshot %}
