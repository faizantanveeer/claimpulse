{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        dbt's default behavior concatenates the target's default schema with
        any model-level +schema config, e.g. profile schema "staging" + model
        config "staging" produces "staging_staging" in Snowflake -- this is
        why early staging builds showed the double-barrelled schema name.

        This override makes the model-level +schema config authoritative on
        its own, so models land exactly in `staging`, `intermediate`, or
        `marts` as named in dbt_project.yml, with no prefixing.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}