select product_id,
    product_name,
    product_category,
    price,
    product_color,
    manufacturing_date,
    expiration_date,
    warranty_period,
    rating,
    weight_grams,
    discount_percentage
from {{ ref('stg_product_rk') }}