{% macro safe_divide(numer, denom) -%}
    CASE
        WHEN {{ denom }} IS NULL OR {{ denom }} = 0 THEN NULL
        ELSE {{ numer }}::numeric / {{ denom }}::numeric
    END
{%- endmacro %}
