{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Override dbt's default behaviour which appends the custom schema to
        the target schema (e.g. "01_bronze_silver").
        With this macro, the schema defined in model configs is used EXACTLY
        as given, without any prefix from the target profile schema.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
