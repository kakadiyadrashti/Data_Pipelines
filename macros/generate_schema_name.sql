{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}        -- use the custom schema exactly
    {%- else -%}
        {{ target.schema }}                    -- otherwise use target schema
    {%- endif -%}
{%- endmacro %}
