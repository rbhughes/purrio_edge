const purr_recent = "__purrRECENT__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

const select = `SELECT * FROM (
  WITH w AS (
    SELECT
      uwi                             AS w_uwi
    FROM well
  ),
  c AS (
    SELECT
      uwi                             AS id_c_uwi,
      LIST(IFNULL(base_depth,         '${purr_null}', CAST(base_depth AS VARCHAR)),         '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_base_depth,
      LIST(IFNULL(base_form,          '${purr_null}', CAST(base_form  AS VARCHAR)),         '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_base_form,
      LIST(IFNULL(completion_date,    '${purr_null}', CAST(completion_date  AS VARCHAR)),   '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_completion_date,
      LIST(IFNULL(completion_form,    '${purr_null}', CAST(completion_form AS VARCHAR)),    '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_completion_form,
      LIST(IFNULL(completion_obs_no,  '${purr_null}', CAST(completion_obs_no AS VARCHAR)),  '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_completion_obs_no,
      LIST(IFNULL(completion_type,    '${purr_null}', CAST(completion_type AS VARCHAR)),    '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_completion_type,
      LIST(IFNULL(remark,             '${purr_null}', CAST(remark AS VARCHAR)),             '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_remark,
      LIST(IFNULL(row_changed_date,   '${purr_null}', CAST(row_changed_date AS VARCHAR)),   '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_row_changed_date,
      LIST(IFNULL(source,             '${purr_null}', CAST(source AS VARCHAR)),             '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_source,
      LIST(IFNULL(top_depth,          '${purr_null}', CAST(top_depth AS VARCHAR)),          '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_top_depth,
      LIST(IFNULL(top_form,           '${purr_null}', CAST(top_form AS VARCHAR)),           '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_top_form,
      LIST(IFNULL(uwi,                '${purr_null}', CAST(uwi AS VARCHAR)),                '${purr_delimiter}' ORDER BY completion_obs_no)  AS c_uwi
    FROM well_completion
    ${purr_recent}
    GROUP BY uwi
  ) 
  SELECT
    w.*,
    c.*
  FROM w
  JOIN c ON w.w_uwi = c.id_c_uwi
) x`;

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // WELL_COMPLETION

  id_c_uwi: {
    ts_type: "string",
  },
  c_base_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_base_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_completion_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_completion_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_completion_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_completion_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_row_changed_date: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_top_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_top_form: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
};


const asset_id_keys = ["w_uwi"];

const default_chunk = 100; // 500

const notes = [];

const order = `ORDER BY w_uwi`;

const prefixes = {
  w_: "well",
  c_: "well_completion",
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
