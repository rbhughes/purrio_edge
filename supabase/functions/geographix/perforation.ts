const purr_recent = "__purrRECENT__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

const select = `SELECT * FROM (
  WITH w AS (
    SELECT
      uwi                        AS w_uwi
    FROM well
  ),
  p AS (
    SELECT 
      uwi                        AS id_p_uwi,
      MAX(row_changed_date)      AS max_row_changed_date,
      LIST(IFNULL(base_depth,                 '${purr_null}', CAST(base_depth AS VARCHAR)),                '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_base_depth,
      LIST(IFNULL(base_form,                  '${purr_null}', CAST(base_form AS VARCHAR)),                 '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_base_form,
      LIST(IFNULL(cluster,                    '${purr_null}', CAST(cluster AS VARCHAR)),                   '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_cluster,
      LIST(IFNULL(completion_obs_no,          '${purr_null}', CAST(completion_obs_no AS VARCHAR)),         '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_completion_obs_no,
      LIST(IFNULL(completion_source,          '${purr_null}', CAST(completion_source AS VARCHAR)),         '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_completion_source,
      LIST(IFNULL(current_status,             '${purr_null}', CAST(current_status AS VARCHAR)),            '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_current_status,
      LIST(IFNULL(gx_base_form_alias,         '${purr_null}', CAST(gx_base_form_alias AS VARCHAR)),        '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_gx_base_form_alias,
      LIST(IFNULL(gx_top_form_alias,          '${purr_null}', CAST(gx_top_form_alias AS VARCHAR)),         '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_gx_top_form_alias,
      LIST(IFNULL(perforation_angle,          '${purr_null}', CAST(perforation_angle AS VARCHAR)),         '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_angle,
      LIST(IFNULL(perforation_count,          '${purr_null}', CAST(perforation_count AS VARCHAR)),         '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_count,
      LIST(IFNULL(perforation_date,           '${purr_null}', CAST(perforation_date AS VARCHAR)),          '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_date,
      LIST(IFNULL(perforation_density,        '${purr_null}', CAST(perforation_density AS VARCHAR)),       '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_density,
      LIST(IFNULL(perforation_diameter,       '${purr_null}', CAST(perforation_diameter AS VARCHAR)),      '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_diameter,
      LIST(IFNULL(perforation_diameter_ouom,  '${purr_null}', CAST(perforation_diameter_ouom AS VARCHAR)), '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_diameter_ouom,
      LIST(IFNULL(perforation_obs_no,         '${purr_null}', CAST(perforation_obs_no AS VARCHAR)),        '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_obs_no,
      LIST(IFNULL(perforation_per_uom,        '${purr_null}', CAST(perforation_per_uom AS VARCHAR)),       '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_per_uom,
      LIST(IFNULL(perforation_phase,          '${purr_null}', CAST(perforation_phase AS VARCHAR)),         '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_phase,
      LIST(IFNULL(perforation_type,           '${purr_null}', CAST(perforation_type AS VARCHAR)),          '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_perforation_type,
      LIST(IFNULL(remark,                     '${purr_null}', CAST(remark AS VARCHAR)),                    '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_remark,
      LIST(IFNULL(row_changed_date,           '${purr_null}', CAST(row_changed_date AS VARCHAR)),          '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_row_changed_date,
      LIST(IFNULL(source,                     '${purr_null}', CAST(source AS VARCHAR)),                    '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_source,
      LIST(IFNULL(stage,                      '${purr_null}', CAST(stage AS VARCHAR)),                     '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_stage,
      LIST(IFNULL(top_depth,                  '${purr_null}', CAST(top_depth AS VARCHAR)),                 '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_top_depth,
      LIST(IFNULL(top_form,                   '${purr_null}', CAST(top_form AS VARCHAR)),                  '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_top_form,
      LIST(IFNULL(uwi,                        '${purr_null}', CAST(uwi AS VARCHAR)),                       '${purr_delimiter}' ORDER BY perforation_obs_no) AS p_uwi
    FROM well_perforation
    ${purr_recent}
    GROUP BY uwi
  )
  SELECT
    w.*,
    p.*
  FROM w
  JOIN p ON w.w_uwi = p.id_p_uwi
) x`;

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // WELL_PERFORATION

  id_p_uwi: {
    ts_type: "string",
  },
  max_row_changed_date: {
    ts_type: "date",
  },

  p_base_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_base_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_cluster: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_completion_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_completion_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_current_status: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_gx_base_form_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_gx_top_form_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_angle: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_count: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_density: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_diameter: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_diameter_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_per_uom: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_phase: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_perforation_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_stage: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_top_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  p_top_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  p_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
};

const asset_id_keys = ["w_uwi"];

const default_chunk = 100; // 400

const notes = [];

const order = `ORDER BY w_uwi`;

const prefixes = {
  w_: "well",
  p_: "well_perforation",
};

const well_id_keys = ["w_uwi"];


///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = () => {
  return {
    asset_id_keys,
    default_chunk,
    notes,
    order,
    purr_delimiter,
    purr_null,
    purr_recent,
    prefixes,
    select,
    well_id_keys,
    xforms
  };
};
