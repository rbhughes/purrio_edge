const purr_recent = "__purrRECENT__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

const select = `SELECT * FROM (
  WITH w AS (
    SELECT
      uwi                       AS w_uwi
    FROM well
  ),
  f AS (
    SELECT
      uwi                       AS id_f_uwi,
      MAX(row_changed_date)     AS max_row_changed_date,
      LIST(IFNULL(dominant_lithology,      '${purr_null}', CAST(dominant_lithology AS VARCHAR)),      '${purr_delimiter}' ORDER BY form_obs_no) AS f_dominant_lithology,
      LIST(IFNULL(fault_name,              '${purr_null}', CAST(fault_name AS VARCHAR)),              '${purr_delimiter}' ORDER BY form_obs_no) AS f_fault_name,
      LIST(IFNULL(faulted_ind,             '${purr_null}', CAST(faulted_ind AS VARCHAR)),             '${purr_delimiter}' ORDER BY form_obs_no) AS f_faulted_ind,
      LIST(IFNULL(form_depth,              '${purr_null}', CAST(form_depth AS VARCHAR)),              '${purr_delimiter}' ORDER BY form_obs_no) AS f_form_depth,
      LIST(IFNULL(form_id,                 '${purr_null}', CAST(form_id AS VARCHAR)),                 '${purr_delimiter}' ORDER BY form_obs_no) AS f_form_id,
      LIST(IFNULL(form_obs_no,             '${purr_null}', CAST(form_obs_no AS VARCHAR)),             '${purr_delimiter}' ORDER BY form_obs_no) AS f_form_obs_no,
      LIST(IFNULL(form_tvd,                '${purr_null}', CAST(form_tvd AS VARCHAR)),                '${purr_delimiter}' ORDER BY form_obs_no) AS f_form_tvd,
      LIST(IFNULL(gap_thickness,           '${purr_null}', CAST(gap_thickness AS VARCHAR)),           '${purr_delimiter}' ORDER BY form_obs_no) AS f_gap_thickness,
      LIST(IFNULL(gx_base_subsea,          '${purr_null}', CAST(gx_base_subsea AS VARCHAR)),          '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_base_subsea,
      LIST(IFNULL(gx_dip,                  '${purr_null}', CAST(gx_dip AS VARCHAR)),                  '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_dip,
      LIST(IFNULL(gx_dip_azimuth,          '${purr_null}', CAST(gx_dip_azimuth AS VARCHAR)),          '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_dip_azimuth,
      LIST(IFNULL(gx_eroded_ind,           '${purr_null}', CAST(gx_eroded_ind AS VARCHAR)),           '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_eroded_ind,
      LIST(IFNULL(gx_exten_id,             '${purr_null}', CAST(gx_exten_id AS VARCHAR)),             '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_exten_id,
      LIST(IFNULL(gx_form_base_depth,      '${purr_null}', CAST(gx_form_base_depth AS VARCHAR)),      '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_form_base_depth,
      LIST(IFNULL(gx_form_base_tvd,        '${purr_null}', CAST(gx_form_base_tvd AS VARCHAR)),        '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_form_base_tvd,
      LIST(IFNULL(gx_form_depth_datum,     '${purr_null}', CAST(gx_form_depth_datum AS VARCHAR)),     '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_form_depth_datum,
      LIST(IFNULL(gx_form_id_alias,        '${purr_null}', CAST(gx_form_id_alias AS VARCHAR)),        '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_form_id_alias,
      LIST(IFNULL(gx_form_top_depth,       '${purr_null}', CAST(gx_form_top_depth AS VARCHAR)),       '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_form_top_depth,
      LIST(IFNULL(gx_form_top_tvd,         '${purr_null}', CAST(gx_form_top_tvd AS VARCHAR)),         '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_form_top_tvd,
      LIST(IFNULL(gx_form_x_coordinate,    '${purr_null}', CAST(gx_form_x_coordinate AS VARCHAR)),    '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_form_x_coordinate,
      LIST(IFNULL(gx_form_y_coordinate,    '${purr_null}', CAST(gx_form_y_coordinate AS VARCHAR)),    '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_form_y_coordinate,
      LIST(IFNULL(gx_gross_thickness,      '${purr_null}', CAST(gx_gross_thickness AS VARCHAR)),      '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_gross_thickness,
      LIST(IFNULL(gx_internal_no,          '${purr_null}', CAST(gx_internal_no AS VARCHAR)),          '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_internal_no,
      LIST(IFNULL(gx_net_thickness,        '${purr_null}', CAST(gx_net_thickness AS VARCHAR)),        '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_net_thickness,
      LIST(IFNULL(gx_porosity,             '${purr_null}', CAST(gx_porosity AS VARCHAR)),             '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_porosity,
      LIST(IFNULL(gx_show,                 '${purr_null}', CAST(gx_show AS VARCHAR)),                 '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_show,
      LIST(IFNULL(gx_strat_column,         '${purr_null}', CAST(gx_strat_column AS VARCHAR)),         '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_strat_column,
      LIST(IFNULL(gx_top_subsea,           '${purr_null}', CAST(gx_top_subsea AS VARCHAR)),           '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_top_subsea,
      LIST(IFNULL(gx_true_strat_thickness, '${purr_null}', CAST(gx_true_strat_thickness AS VARCHAR)), '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_true_strat_thickness,
      LIST(IFNULL(gx_true_vert_thickness,  '${purr_null}', CAST(gx_true_vert_thickness AS VARCHAR)),  '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_true_vert_thickness,
      LIST(IFNULL(gx_user1,                '${purr_null}', CAST(gx_user1 AS VARCHAR)),                '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_user1,
      LIST(IFNULL(gx_user2,                '${purr_null}', CAST(gx_user2 AS VARCHAR)),                '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_user2,
      LIST(IFNULL(gx_user3,                '${purr_null}', CAST(gx_user3 AS VARCHAR)),                '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_user3,
      LIST(IFNULL(gx_user4,                '${purr_null}', CAST(gx_user4 AS VARCHAR)),                '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_user4,
      LIST(IFNULL(gx_user5,                '${purr_null}', CAST(gx_user5 AS VARCHAR)),                '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_user5,
      LIST(IFNULL(gx_user6,                '${purr_null}', CAST(gx_user6 AS VARCHAR)),                '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_user6,
      LIST(IFNULL(gx_vendor_no,            '${purr_null}', CAST(gx_vendor_no AS VARCHAR)),            '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_vendor_no,
      LIST(IFNULL(gx_wellbore_angle,       '${purr_null}', CAST(gx_wellbore_angle AS VARCHAR)),       '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_wellbore_angle,
      LIST(IFNULL(gx_wellbore_azimuth,     '${purr_null}', CAST(gx_wellbore_azimuth AS VARCHAR)),     '${purr_delimiter}' ORDER BY form_obs_no) AS f_gx_wellbore_azimuth,
      LIST(IFNULL(percent_thickness,       '${purr_null}', CAST(percent_thickness AS VARCHAR)),       '${purr_delimiter}' ORDER BY form_obs_no) AS f_percent_thickness,
      LIST(IFNULL(pick_location,           '${purr_null}', CAST(pick_location AS VARCHAR)),           '${purr_delimiter}' ORDER BY form_obs_no) AS f_pick_location,
      LIST(IFNULL(pick_qualifier,          '${purr_null}', CAST(pick_qualifier AS VARCHAR)),          '${purr_delimiter}' ORDER BY form_obs_no) AS f_pick_qualifier,
      LIST(IFNULL(pick_quality,            '${purr_null}', CAST(pick_quality AS VARCHAR)),            '${purr_delimiter}' ORDER BY form_obs_no) AS f_pick_quality,
      LIST(IFNULL(pick_type,               '${purr_null}', CAST(pick_type AS VARCHAR)),               '${purr_delimiter}' ORDER BY form_obs_no) AS f_pick_type,
      LIST(IFNULL(public,                  '${purr_null}', CAST(public AS VARCHAR)),                  '${purr_delimiter}' ORDER BY form_obs_no) AS f_public,
      LIST(IFNULL(remark,                  '${purr_null}', CAST(remark AS VARCHAR)),                  '${purr_delimiter}' ORDER BY form_obs_no) AS f_remark,
      LIST(IFNULL(row_changed_date,        '${purr_null}', CAST(row_changed_date AS VARCHAR)),        '${purr_delimiter}' ORDER BY form_obs_no) AS f_row_changed_date,
      LIST(IFNULL(source,                  '${purr_null}', CAST(source AS VARCHAR)),                  '${purr_delimiter}' ORDER BY form_obs_no) AS f_source,
      LIST(IFNULL(unc_fault_obs_no,        '${purr_null}', CAST(unc_fault_obs_no AS VARCHAR)),        '${purr_delimiter}' ORDER BY form_obs_no) AS f_unc_fault_obs_no,
      LIST(IFNULL(unc_fault_source,        '${purr_null}', CAST(unc_fault_source AS VARCHAR)),        '${purr_delimiter}' ORDER BY form_obs_no) AS f_unc_fault_source,
      LIST(IFNULL(unconformity_name,       '${purr_null}', CAST(unconformity_name AS VARCHAR)),       '${purr_delimiter}' ORDER BY form_obs_no) AS f_unconformity_name,
      LIST(IFNULL(uwi,                     '${purr_null}', CAST(uwi AS VARCHAR)),                     '${purr_delimiter}' ORDER BY form_obs_no) AS f_uwi
    FROM well_formation
    ${purr_recent}
    GROUP BY uwi
  )
  SELECT 
    w.*,
    f.*
  FROM w
  JOIN f ON
    f.id_f_uwi = w.w_uwi
) x`;

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // WELL_FORMATION

  id_f_uwi: {
    ts_type: "string",
  },
  max_row_changed_date: {
    ts_type: "date",
  },

  f_dominant_lithology: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_fault_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_faulted_ind: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_form_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_form_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_form_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_form_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gap_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_base_subsea: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_dip: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_dip_azimuth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_eroded_ind: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_exten_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_base_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_base_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_depth_datum: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_id_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_top_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_top_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_x_coordinate: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_form_y_coordinate: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_gross_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_internal_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_net_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_porosity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_show: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_strat_column: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_top_subsea: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_true_strat_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_true_vert_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user1: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user2: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user3: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user4: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user5: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_user6: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_vendor_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_wellbore_angle: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_gx_wellbore_azimuth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_percent_thickness: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_pick_location: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_pick_qualifier: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_pick_quality: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_pick_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_public: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  f_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_unc_fault_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_unc_fault_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_unconformity_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
};

const asset_id_keys = ["w_uwi"];
//const asset_id_keys = ["w_uwi", "f_form_id", "f_form_obs_no", "f_source"];

const default_chunk = 100; // 200

const notes = [`ignored: fault.fault_id, r_source.source`];

const order = `ORDER BY w_uwi`;

const prefixes = {
  w_: "well",
  f_: "well_formation",
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
