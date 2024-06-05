const purr_recent = "__purrRECENT__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

const select = `SELECT * FROM (
  WITH w AS (
    SELECT
      uwi                        AS w_uwi
    FROM well
  ),
  c AS (
    SELECT
      base_depth                  AS c_base_depth,
      base_depth_ouom             AS c_base_depth_ouom,
      core_id                     AS c_core_id,
      core_type                   AS c_core_type,
      gx_primary_core_form_alias  AS c_gx_primary_core_form_alias,
      gx_qualifying_field         AS c_gx_qualifying_field,
      gx_remark                   AS c_gx_remark,
      gx_user1                    AS c_gx_user1,
      primary_core_form           AS c_primary_core_form,
      recovered_amt               AS c_recovered_amt,
      recovery_amt_ouom           AS c_recovery_amt_ouom,
      recovery_date               AS c_recovery_date,
      reported_core_number        AS c_reported_core_number,
      row_changed_date            AS c_row_changed_date,
      source                      AS c_source,
      top_depth                   AS c_top_depth,
      top_depth_ouom              AS c_top_depth_ouom,
      uwi                         AS c_uwi
    FROM well_core
    ${purr_recent}
  ),
  s AS (
    SELECT
      core_id                    AS id_core_id,
      source                     AS id_source,
      uwi                        AS id_uwi,
      MAX(row_changed_date)      AS max_row_changed_date,
      LIST(IFNULL(analysis_obs_no,          '${purr_null}', CAST(analysis_obs_no AS VARCHAR)),          '${purr_delimiter}' ORDER BY core_id) AS s_analysis_obs_no,
      LIST(IFNULL(core_id,                  '${purr_null}', CAST(core_id AS VARCHAR)),                  '${purr_delimiter}' ORDER BY core_id) AS s_core_id,
      LIST(IFNULL(gas_sat_vol,              '${purr_null}', CAST(gas_sat_vol AS VARCHAR)),              '${purr_delimiter}' ORDER BY core_id) AS s_gas_sat_vol,
      LIST(IFNULL(grain_density,            '${purr_null}', CAST(grain_density AS VARCHAR)),            '${purr_delimiter}' ORDER BY core_id) AS s_grain_density,
      LIST(IFNULL(grain_density_ouom,       '${purr_null}', CAST(grain_density_ouom AS VARCHAR)),       '${purr_delimiter}' ORDER BY core_id) AS s_grain_density_ouom,
      LIST(IFNULL(gx_base_depth,            '${purr_null}', CAST(gx_base_depth AS VARCHAR)),            '${purr_delimiter}' ORDER BY core_id) AS s_gx_base_depth,
      LIST(IFNULL(gx_bulk_density,          '${purr_null}', CAST(gx_bulk_density AS VARCHAR)),          '${purr_delimiter}' ORDER BY core_id) AS s_gx_bulk_density,
      LIST(IFNULL(gx_formation,             '${purr_null}', CAST(gx_formation AS VARCHAR)),             '${purr_delimiter}' ORDER BY core_id) AS s_gx_formation,
      LIST(IFNULL(gx_formation_alias,       '${purr_null}', CAST(gx_formation_alias AS VARCHAR)),       '${purr_delimiter}' ORDER BY core_id) AS s_gx_formation_alias,
      LIST(IFNULL(gx_gamma_ray,             '${purr_null}', CAST(gx_gamma_ray AS VARCHAR)),             '${purr_delimiter}' ORDER BY core_id) AS s_gx_gamma_ray,
      LIST(IFNULL(gx_lithology_desc,        '${purr_null}', CAST(gx_lithology_desc AS VARCHAR)),        '${purr_delimiter}' ORDER BY core_id) AS s_gx_lithology_desc,
      LIST(IFNULL(gx_poissons_ratio,        '${purr_null}', CAST(gx_poissons_ratio AS VARCHAR)),        '${purr_delimiter}' ORDER BY core_id) AS s_gx_poissons_ratio,
      LIST(IFNULL(gx_remark,                '${purr_null}', CAST(gx_remark AS VARCHAR)),                '${purr_delimiter}' ORDER BY core_id) AS s_gx_remark,
      LIST(IFNULL(gx_resistivity,           '${purr_null}', CAST(gx_resistivity AS VARCHAR)),           '${purr_delimiter}' ORDER BY core_id) AS s_gx_resistivity,
      LIST(IFNULL(gx_shift_depth,           '${purr_null}', CAST(gx_shift_depth AS VARCHAR)),           '${purr_delimiter}' ORDER BY core_id) AS s_gx_shift_depth,
      LIST(IFNULL(gx_show_type,             '${purr_null}', CAST(gx_show_type AS VARCHAR)),             '${purr_delimiter}' ORDER BY core_id) AS s_gx_show_type,
      LIST(IFNULL(gx_toc,                   '${purr_null}', CAST(gx_toc AS VARCHAR)),                   '${purr_delimiter}' ORDER BY core_id) AS s_gx_toc,
      LIST(IFNULL(gx_total_clay,            '${purr_null}', CAST(gx_total_clay AS VARCHAR)),            '${purr_delimiter}' ORDER BY core_id) AS s_gx_total_clay,
      LIST(IFNULL(gx_vitrinite_reflectance, '${purr_null}', CAST(gx_vitrinite_reflectance AS VARCHAR)), '${purr_delimiter}' ORDER BY core_id) AS s_gx_vitrinite_reflectance,
      LIST(IFNULL(gx_youngs_modulus,        '${purr_null}', CAST(gx_youngs_modulus AS VARCHAR)),        '${purr_delimiter}' ORDER BY core_id) AS s_gx_youngs_modulus,
      LIST(IFNULL(k90,                      '${purr_null}', CAST(k90 AS VARCHAR)),                      '${purr_delimiter}' ORDER BY core_id) AS s_k90,
      LIST(IFNULL(k90_ouom,                 '${purr_null}', CAST(k90_ouom AS VARCHAR)),                 '${purr_delimiter}' ORDER BY core_id) AS s_k90_ouom,
      LIST(IFNULL(kmax,                     '${purr_null}', CAST(kmax AS VARCHAR)),                     '${purr_delimiter}' ORDER BY core_id) AS s_kmax,
      LIST(IFNULL(kmax_ouom,                '${purr_null}', CAST(kmax_ouom AS VARCHAR)),                '${purr_delimiter}' ORDER BY core_id) AS s_kmax_ouom,
      LIST(IFNULL(kvert,                    '${purr_null}', CAST(kvert AS VARCHAR)),                    '${purr_delimiter}' ORDER BY core_id) AS s_kvert,
      LIST(IFNULL(kvert_ouom,               '${purr_null}', CAST(kvert_ouom AS VARCHAR)),               '${purr_delimiter}' ORDER BY core_id) AS s_kvert_ouom,
      LIST(IFNULL(oil_sat,                  '${purr_null}', CAST(oil_sat AS VARCHAR)),                  '${purr_delimiter}' ORDER BY core_id) AS s_oil_sat,
      LIST(IFNULL(pore_volume_oil_sat,      '${purr_null}', CAST(pore_volume_oil_sat AS VARCHAR)),      '${purr_delimiter}' ORDER BY core_id) AS s_pore_volume_oil_sat,
      LIST(IFNULL(pore_volume_water_sat,    '${purr_null}', CAST(pore_volume_water_sat AS VARCHAR)),    '${purr_delimiter}' ORDER BY core_id) AS s_pore_volume_water_sat,
      LIST(IFNULL(porosity,                 '${purr_null}', CAST(porosity AS VARCHAR)),                 '${purr_delimiter}' ORDER BY core_id) AS s_porosity,
      LIST(IFNULL(row_changed_date,         '${purr_null}', CAST(row_changed_date AS VARCHAR)),         '${purr_delimiter}' ORDER BY core_id) AS s_row_changed_date,
      LIST(IFNULL(sample_id,                '${purr_null}', CAST(sample_id AS VARCHAR)),                '${purr_delimiter}' ORDER BY core_id) AS s_sample_id,
      LIST(IFNULL(sample_number,            '${purr_null}', CAST(sample_number AS VARCHAR)),            '${purr_delimiter}' ORDER BY core_id) AS s_sample_number,
      LIST(IFNULL(source,                   '${purr_null}', CAST(source AS VARCHAR)),                   '${purr_delimiter}' ORDER BY core_id) AS s_source,
      LIST(IFNULL(top_depth,                '${purr_null}', CAST(top_depth AS VARCHAR)),                '${purr_delimiter}' ORDER BY core_id) AS s_top_depth,
      LIST(IFNULL(top_depth_ouom,           '${purr_null}', CAST(top_depth_ouom AS VARCHAR)),           '${purr_delimiter}' ORDER BY core_id) AS s_top_depth_ouom,
      LIST(IFNULL(uwi,                      '${purr_null}', CAST(uwi AS VARCHAR)),                      '${purr_delimiter}' ORDER BY core_id) AS s_uwi,
      LIST(IFNULL(water_sat,                '${purr_null}', CAST(water_sat AS VARCHAR)),                '${purr_delimiter}' ORDER BY core_id) AS s_water_sat
    FROM well_core_sample_anal
    GROUP BY id_uwi, id_source, id_core_id
  )
  SELECT
    w.*,
    c.*,
    s.*
  FROM w
  JOIN c ON c.c_uwi = w.w_uwi
  LEFT OUTER JOIN s
    ON c.c_uwi = s.id_uwi
    AND c.c_source = s.id_source
    AND c.c_core_id = s.id_core_id
) x`;


const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // WELL_CORE_SAMPLE_ANAL

  id_core_id: {
    ts_type: "string",
  },
  id_source: {
    ts_type: "string",
  },
  id_uwi: {
    ts_type: "string",
  },
  max_row_changed_date: {
    ts_type: "date",
  },

  s_analysis_obs_no: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_core_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gas_sat_vol: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_grain_density: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_grain_density_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_base_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_bulk_density: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_formation: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_formation_alias: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_gamma_ray: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_lithology_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_poissons_ratio: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_resistivity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_shift_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_show_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_gx_toc: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_total_clay: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_vitrinite_reflectance: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_gx_youngs_modulus: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_k90: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_k90_ouom: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_kmax: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_kmax_ouom: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_kvert: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_kvert_ouom: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_oil_sat: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_pore_volume_oil_sat: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_pore_volume_water_sat: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_porosity: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  s_sample_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_sample_number: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_top_depth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_top_depth_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  s_uwi: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  s_water_sat: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },

  // WELL_CORE

  c_base_depth: {
    ts_type: "number",
  },
  c_base_depth_ouom: {
    ts_type: "string",
  },
  c_core_id: {
    ts_type: "string",
  },
  c_core_type: {
    ts_type: "string",
  },
  c_gx_primary_core_form_alias: {
    ts_type: "string",
  },
  c_gx_qualifying_field: {
    ts_type: "string",
  },
  c_gx_remark: {
    ts_type: "string",
  },
  c_gx_user1: {
    ts_type: "string",
  },
  c_primary_core_form: {
    ts_type: "string",
  },
  c_recovered_amt: {
    ts_type: "number",
  },
  c_recovery_amt_ouom: {
    ts_type: "string",
  },
  c_recovery_date: {
    ts_type: "date",
  },
  c_reported_core_number: {
    ts_type: "string",
  },
  c_row_changed_date: {
    ts_type: "date",
  },
  c_source: {
    ts_type: "string",
  },
  c_top_depth: {
    ts_type: "number",
  },
  c_top_depth_ouom: {
    ts_type: "string",
  },
  c_uwi: {
    ts_type: "string",
  },
};

const asset_id_keys = ["w_uwi", "c_source", "c_core_id"];

const default_chunk = 100; // 200

const notes = [
  'Omitted most "gx_user*" columns in WELL_CORE_SAMPLE_ANAL to conserve memory.'
]

const order = `ORDER BY w_uwi, c_source, c_core_id`;

const prefixes = {
  w_: "well",
  s_: "well_core_sample_anal",
  c_: "well_core",
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
