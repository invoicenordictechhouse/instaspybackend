-- TODO: Clean raw Meta data for further processing

WITH raw_meta_data AS (
  SELECT
      *
  FROM
      `your_project.your_raw_dataset.meta_raw_data`
)

