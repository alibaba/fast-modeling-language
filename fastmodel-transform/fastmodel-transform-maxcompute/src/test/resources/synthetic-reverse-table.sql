CREATE TABLE IF NOT EXISTS example_reverse_table (
    example_id BIGINT COMMENT 'Synthetic identifier',
    example_label STRING COMMENT 'Synthetic label',
    example_score DOUBLE COMMENT 'Synthetic score',
    example_tags ARRAY<STRING> COMMENT 'Synthetic tags',
    example_categories ARRAY<STRING> COMMENT 'Synthetic categories',
    example_count BIGINT COMMENT 'Synthetic count'
) COMMENT 'Synthetic primitive and array regression'
PARTITIONED BY (example_day STRING COMMENT 'Synthetic partition')
LIFECYCLE 7;
