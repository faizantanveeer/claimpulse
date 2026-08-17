{% macro normalize_text(column_name) %}
    -- Collapses repeated whitespace and trims -- used to catch the
    -- "same name, typo'd spacing" duplicate providers seen in raw data.
    trim(regexp_replace({{ column_name }}, '\\s+', ' '))
{% endmacro %}