const purr_recent = "__purrRECENT__"
const purr_null = "__purrNULL__"
const purr_delimiter = "__purrDELIMITER__";

const select = `SELECT * FROM (
  WITH w AS (
    SELECT
      uwi                   AS w_uwi
    FROM well
  ),
  d AS (
    SELECT 
      uwi                   AS id_d_uwi,
      survey_id             AS id_d_survey_id,
      source                AS id_d_source,
      'actual'              AS id_d_kind,
      MAX(row_changed_date) AS max_row_changed_date,
      LIST(IFNULL(azimuth,              '${purr_null}', CAST(azimuth AS VARCHAR)),               '${purr_delimiter}' ORDER BY station_md)  AS d_azimuth,
      LIST(IFNULL(azimuth_ouom,         '${purr_null}', azimuth_ouom),                           '${purr_delimiter}' ORDER BY station_md)  AS d_azimuth_ouom,
      LIST(IFNULL(ew_direction,         '${purr_null}', ew_direction),                           '${purr_delimiter}' ORDER BY station_md)  AS d_ew_direction,
      LIST(IFNULL(gx_closure,           '${purr_null}', gx_closure),                             '${purr_delimiter}' ORDER BY station_md)  AS d_gx_closure,
      LIST(IFNULL(gx_station_latitude,  '${purr_null}', CAST(gx_station_latitude AS VARCHAR)),   '${purr_delimiter}' ORDER BY station_md)  AS d_gx_station_latitude,
      LIST(IFNULL(gx_station_longitude, '${purr_null}', CAST(gx_station_longitude AS VARCHAR)),  '${purr_delimiter}' ORDER BY station_md)  AS d_gx_station_longitude,
      LIST(IFNULL(inclination,          '${purr_null}', CAST(inclination AS VARCHAR)),           '${purr_delimiter}' ORDER BY station_md)  AS d_inclination,
      LIST(IFNULL(inclination_ouom,     '${purr_null}', inclination_ouom),                       '${purr_delimiter}' ORDER BY station_md)  AS d_inclination_ouom,
      LIST(IFNULL(ns_direction,         '${purr_null}', ns_direction),                           '${purr_delimiter}' ORDER BY station_md)  AS d_ns_direction,
      LIST(IFNULL(row_changed_date,     '${purr_null}', CAST(row_changed_date AS VARCHAR)),      '${purr_delimiter}' ORDER BY station_md)  AS d_row_changed_date,
      LIST(IFNULL(source,               '${purr_null}', source),                                 '${purr_delimiter}' ORDER BY station_md)  AS d_source,
      LIST(IFNULL(station_id,           '${purr_null}', station_id),                             '${purr_delimiter}' ORDER BY station_md)  AS d_station_id,
      LIST(IFNULL(station_md,           '${purr_null}', CAST(station_md AS VARCHAR)),            '${purr_delimiter}' ORDER BY station_md)  AS d_station_md,
      LIST(IFNULL(station_md_ouom,      '${purr_null}', station_md_ouom),                        '${purr_delimiter}' ORDER BY station_md)  AS d_station_md_ouom,
      LIST(IFNULL(station_tvd,          '${purr_null}', CAST(station_tvd AS VARCHAR)),           '${purr_delimiter}' ORDER BY station_md)  AS d_station_tvd,
      LIST(IFNULL(station_tvd_ouom,     '${purr_null}', station_tvd_ouom),                       '${purr_delimiter}' ORDER BY station_md)  AS d_station_tvd_ouom,
      LIST(IFNULL(survey_id,            '${purr_null}', survey_id),                              '${purr_delimiter}' ORDER BY station_md)  AS d_survey_id,
      LIST(IFNULL(uwi,                  '${purr_null}', uwi),                                    '${purr_delimiter}' ORDER BY station_md)  AS d_uwi,
      LIST(IFNULL(x_offset,             '${purr_null}', CAST(x_offset AS VARCHAR)),              '${purr_delimiter}' ORDER BY station_md)  AS d_x_offset,
      LIST(IFNULL(x_offset_ouom,        '${purr_null}', x_offset_ouom),                          '${purr_delimiter}' ORDER BY station_md)  AS d_x_offset_ouom,
      LIST(IFNULL(y_offset,             '${purr_null}', CAST(y_offset AS VARCHAR)),              '${purr_delimiter}' ORDER BY station_md)  AS d_y_offset,
      LIST(IFNULL(y_offset_ouom,        '${purr_null}', y_offset_ouom),                          '${purr_delimiter}' ORDER BY station_md)  AS d_y_offset_ouom
    FROM well_dir_srvy_station
    GROUP BY uwi, survey_id, source
    UNION
    SELECT 
      uwi                   AS id_d_uwi,
      survey_id             AS id_d_survey_id,
      source                AS id_d_source,
      'proposed'            AS id_d_kind,
      MAX(row_changed_date) AS max_row_changed_date,
      LIST(IFNULL(azimuth,              '${purr_null}', CAST(azimuth AS VARCHAR)),               '${purr_delimiter}' ORDER BY station_md)  AS d_azimuth,
      LIST(IFNULL(azimuth_ouom,         '${purr_null}', azimuth_ouom),                           '${purr_delimiter}' ORDER BY station_md)  AS d_azimuth_ouom,
      LIST(IFNULL(ew_direction,         '${purr_null}', ew_direction),                           '${purr_delimiter}' ORDER BY station_md)  AS d_ew_direction,
      LIST(IFNULL(gx_closure,           '${purr_null}', gx_closure),                             '${purr_delimiter}' ORDER BY station_md)  AS d_gx_closure,
      LIST(IFNULL(gx_station_latitude,  '${purr_null}', CAST(gx_station_latitude AS VARCHAR)),   '${purr_delimiter}' ORDER BY station_md)  AS d_gx_station_latitude,
      LIST(IFNULL(gx_station_longitude, '${purr_null}', CAST(gx_station_longitude AS VARCHAR)),  '${purr_delimiter}' ORDER BY station_md)  AS d_gx_station_longitude,
      LIST(IFNULL(inclination,          '${purr_null}', CAST(inclination AS VARCHAR)),           '${purr_delimiter}' ORDER BY station_md)  AS d_inclination,
      LIST(IFNULL(inclination_ouom,     '${purr_null}', inclination_ouom),                       '${purr_delimiter}' ORDER BY station_md)  AS d_inclination_ouom,
      LIST(IFNULL(ns_direction,         '${purr_null}', ns_direction),                           '${purr_delimiter}' ORDER BY station_md)  AS d_ns_direction,
      LIST(IFNULL(row_changed_date,     '${purr_null}', CAST(row_changed_date AS VARCHAR)),      '${purr_delimiter}' ORDER BY station_md)  AS d_row_changed_date,
      LIST(IFNULL(source,               '${purr_null}', source),                                 '${purr_delimiter}' ORDER BY station_md)  AS d_source,
      LIST(IFNULL(station_id,           '${purr_null}', station_id),                             '${purr_delimiter}' ORDER BY station_md)  AS d_station_id,
      LIST(IFNULL(station_md,           '${purr_null}', CAST(station_md AS VARCHAR)),            '${purr_delimiter}' ORDER BY station_md)  AS d_station_md,
      LIST(IFNULL(station_md_ouom,      '${purr_null}', station_md_ouom),                        '${purr_delimiter}' ORDER BY station_md)  AS d_station_md_ouom,
      LIST(IFNULL(station_tvd,          '${purr_null}', CAST(station_tvd AS VARCHAR)),           '${purr_delimiter}' ORDER BY station_md)  AS d_station_tvd,
      LIST(IFNULL(station_tvd_ouom,     '${purr_null}', station_tvd_ouom),                       '${purr_delimiter}' ORDER BY station_md)  AS d_station_tvd_ouom,
      LIST(IFNULL(survey_id,            '${purr_null}', survey_id),                              '${purr_delimiter}' ORDER BY station_md)  AS d_survey_id,
      LIST(IFNULL(uwi,                  '${purr_null}', uwi),                                    '${purr_delimiter}' ORDER BY station_md)  AS d_uwi,
      LIST(IFNULL(x_offset,             '${purr_null}', CAST(x_offset AS VARCHAR)),              '${purr_delimiter}' ORDER BY station_md)  AS d_x_offset,
      LIST(IFNULL(x_offset_ouom,        '${purr_null}', x_offset_ouom),                          '${purr_delimiter}' ORDER BY station_md)  AS d_x_offset_ouom,
      LIST(IFNULL(y_offset,             '${purr_null}', CAST(y_offset AS VARCHAR)),              '${purr_delimiter}' ORDER BY station_md)  AS d_y_offset,
      LIST(IFNULL(y_offset_ouom,        '${purr_null}', y_offset_ouom),                          '${purr_delimiter}' ORDER BY station_md)  AS d_y_offset_ouom
    FROM well_dir_proposed_srvy_station
    GROUP BY uwi, survey_id, source
  ),
  s AS (
    SELECT
      'actual'                   AS s_kind,
      s.azimuth_north_type       AS s_azimuth_north_type,
      s.base_depth               AS s_base_depth,
      s.base_depth_ouom          AS s_base_depth_ouom,
      s.calculation_required     AS s_calculation_required,
      s.compute_method           AS s_compute_method,
      s.ew_magnetic_declination  AS s_ew_magnetic_declination,
      s.grid_system_id           AS s_grid_system_id,
      null                       AS s_gx_active,
      s.gx_base_e_w_offset       AS s_gx_base_e_w_offset,
      s.gx_base_latitude         AS s_gx_base_latitude,
      s.gx_base_location_string  AS s_gx_base_location_string,
      s.gx_base_longitude        AS s_gx_base_longitude,
      s.gx_base_n_s_offset       AS s_gx_base_n_s_offset,
      s.gx_base_tvd              AS s_gx_base_tvd,
      s.gx_closure               AS s_gx_closure,
      s.gx_footage               AS s_gx_footage,
      s.gx_kop_e_w_offset        AS s_gx_kop_e_w_offset,
      s.gx_kop_latitude          AS s_gx_kop_latitude,
      s.gx_kop_longitude         AS s_gx_kop_longitude,
      s.gx_kop_md                AS s_gx_kop_md,
      s.gx_kop_n_s_offset        AS s_gx_kop_n_s_offset,
      s.gx_kop_tvd               AS s_gx_kop_tvd,
      null                       AS s_gx_lp_e_w_offset,
      null                       AS s_gx_lp_n_s_offset,
      null                       AS s_gx_lp_tvd,
      null                       AS s_gx_proposed_well_blob,
      null                       AS s_gx_scenario_name,
      s.magnetic_declination     AS s_magnetic_declination,
      s.north_reference          AS s_north_reference,
      s.offset_north_type        AS s_offset_north_type,
      s.record_mode              AS s_record_mode,
      s.remark                   AS s_remark,
      s.row_changed_date         AS s_row_changed_date,
      s.source                   AS s_source,
      s.source_document          AS s_source_document,
      s.survey_company           AS s_survey_company,
      s.survey_date              AS s_survey_date,
      s.survey_id                AS s_survey_id,
      s.survey_quality           AS s_survey_quality,
      s.survey_type              AS s_survey_type,
      s.top_depth                AS s_top_depth,
      s.top_depth_ouom           AS s_top_depth_ouom,
      s.uwi                      AS s_uwi
    FROM well_dir_srvy s
    WHERE s.uwi IN (SELECT DISTINCT(uwi) FROM well_dir_srvy_station)
    ${purr_recent}
    UNION
    SELECT
      'proposed'                 AS s_kind,
      s.azimuth_north_type       AS s_azimuth_north_type,
      s.base_depth               AS s_base_depth,
      s.base_depth_ouom          AS s_base_depth_ouom,
      s.calculation_required     AS s_calculation_required,
      s.compute_method           AS s_compute_method,
      s.ew_magnetic_declination  AS s_ew_magnetic_declination,
      s.grid_system_id           AS s_grid_system_id,
      s.gx_active                AS s_gx_active,
      s.gx_base_e_w_offset       AS s_gx_base_e_w_offset,
      s.gx_base_latitude         AS s_gx_base_latitude,
      s.gx_base_location_string  AS s_gx_base_location_string,
      s.gx_base_longitude        AS s_gx_base_longitude,
      s.gx_base_n_s_offset       AS s_gx_base_n_s_offset,
      s.gx_base_tvd              AS s_gx_base_tvd,
      s.gx_closure               AS s_gx_closure,
      s.gx_footage               AS s_gx_footage,
      s.gx_kop_e_w_offset        AS s_gx_kop_e_w_offset,
      s.gx_kop_latitude          AS s_gx_kop_latitude,
      s.gx_kop_longitude         AS s_gx_kop_longitude,
      s.gx_kop_md                AS s_gx_kop_md,
      s.gx_kop_n_s_offset        AS s_gx_kop_n_s_offset,
      s.gx_kop_tvd               AS s_gx_kop_tvd,
      s.gx_lp_e_w_offset         AS s_gx_lp_e_w_offset,
      s.gx_lp_n_s_offset         AS s_gx_lp_n_s_offset,
      s.gx_lp_tvd                AS s_gx_lp_tvd,
      s.gx_proposed_well_blob    AS s_gx_proposed_well_blob,
      s.gx_scenario_name         AS s_gx_scenario_name,
      s.magnetic_declination     AS s_magnetic_declination,
      s.north_reference          AS s_north_reference,
      s.offset_north_type        AS s_offset_north_type,
      s.record_mode              AS s_record_mode,
      s.remark                   AS s_remark,
      s.row_changed_date         AS s_row_changed_date,
      s.source                   AS s_source,
      s.source_document          AS s_source_document,
      s.survey_company           AS s_survey_company,
      s.survey_date              AS s_survey_date,
      s.survey_id                AS s_survey_id,
      s.survey_quality           AS s_survey_quality,
      s.survey_type              AS s_survey_type,
      s.top_depth                AS s_top_depth,
      s.top_depth_ouom           AS s_top_depth_ouom,
      s.uwi                      AS s_uwi
    FROM well_dir_proposed_srvy s
    WHERE s.uwi IN (SELECT DISTINCT(uwi) FROM well_dir_proposed_srvy_station)
    ${purr_recent}
  )
  SELECT
    w.*,
    s.*,
    d.*
  FROM d
  JOIN s
    ON s.s_source = d.id_d_source
    AND s.s_survey_id = d.id_d_survey_id
    AND s.s_uwi = d.id_d_uwi
    AND s.s_kind = d.id_d_kind
  JOIN w ON w.w_uwi = s.s_uwi
) x`;

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // WELL_DIR_SRVY + WELL_DIR_PROPOSED_SRVY

  s_kind: {
    ts_type: "string",
  },
  s_azimuth_north_type: {
    ts_type: "string",
  },
  s_base_depth: {
    ts_type: "number",
  },
  s_base_depth_ouom: {
    ts_type: "string",
  },
  s_calculation_required: {
    ts_type: "number",
  },
  s_compute_method: {
    ts_type: "string",
  },
  s_ew_magnetic_declination: {
    ts_type: "string",
  },
  s_grid_system_id: {
    ts_type: "string",
  },
  s_gx_active: {
    ts_type: "number",
  },
  s_gx_base_e_w_offset: {
    ts_type: "number",
  },
  s_gx_base_latitude: {
    ts_type: "number",
  },
  s_gx_base_location_string: {
    ts_type: "string",
  },
  s_gx_base_longitude: {
    ts_type: "number",
  },
  s_gx_base_n_s_offset: {
    ts_type: "number",
  },
  s_gx_base_tvd: {
    ts_type: "number",
  },
  s_gx_closure: {
    ts_type: "number",
  },
  s_gx_footage: {
    ts_type: "string",
  },
  s_gx_kop_e_w_offset: {
    ts_type: "number",
  },
  s_gx_kop_latitude: {
    ts_type: "number",
  },
  s_gx_kop_longitude: {
    ts_type: "number",
  },
  s_gx_kop_md: {
    ts_type: "number",
  },
  s_gx_kop_n_s_offset: {
    ts_type: "number",
  },
  s_gx_kop_tvd: {
    ts_type: "number",
  },
  s_gx_lp_e_w_offset: {
    ts_type: "number",
  },
  s_gx_lp_n_s_offset: {
    ts_type: "number",
  },
  s_gx_lp_tvd: {
    ts_type: "number",
  },
  s_gx_proposed_well_blob: {
    ts_type: "string",
    xform: "blob_to_hex",
  },
  s_gx_scenario_name: {
    ts_type: "string",
  },
  s_magnetic_declination: {
    ts_type: "string",
  },
  s_north_reference: {
    ts_type: "string",
  },
  s_offset_north_type: {
    ts_type: "string",
  },
  s_record_mode: {
    ts_type: "string",
  },
  s_remark: {
    ts_type: "string",
  },
  s_row_changed_date: {
    ts_type: "date",
  },
  s_source: {
    ts_type: "string",
  },
  s_source_document: {
    ts_type: "string",
  },
  s_survey_company: {
    ts_type: "string",
  },
  s_survey_date: {
    ts_type: "date",
  },
  s_survey_id: {
    ts_type: "string",
  },
  s_survey_quality: {
    ts_type: "string",
  },
  s_survey_type: {
    ts_type: "string",
  },
  s_top_depth: {
    ts_type: "number",
  },
  s_top_depth_ouom: {
    ts_type: "string",
  },
  s_uwi: {
    ts_type: "string",
  },

  // WELL_DIR_SRVY_STATION + WELL_DIR_PROPOSED_SRVY_STATION

  id_d_uwi: {
    ts_type: "string",
  },
  id_d_survey_id: {
    ts_type: "string",
  },
  id_d_source: {
    ts_type: "string",
  },
  id_d_kind: {
    ts_type: "string",
  },
  max_row_changed_date: {
    ts_type: "date",
  },
  d_azimuth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  d_azimuth_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_ew_direction: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_gx_closure: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_gx_station_latitude: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  d_gx_station_longitude: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  d_inclination: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  d_inclination_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_ns_direction: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  d_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_station_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_station_md: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  d_station_md_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_station_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  d_station_tvd_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_survey_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_x_offset: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  d_x_offset_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  d_y_offset: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  d_y_offset_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
};

const asset_id_keys = ["w_uwi", "id_d_survey_id", "id_d_uwi", "id_d_kind"];

const default_chunk = 100; // 500

const notes = [
  "Yes, this will be quite slow.",
  "The recency WHERE clause gets applied to WELL_DIR_SRVY and WELL_DIR_PROPOSED_SRVY.",
  "The 'proposed' survey/station tables get normlized in the UNION",
  "Ignores well_dir_proposed_srvy_station.gx_dls"
];

const order = `ORDER BY w_uwi, id_d_survey_id, id_d_uwi, id_d_kind`;

const prefixes = {
  w_: "well",
  d_: "well_dir_srvy_station",
  s_: "well_dir_srvy",
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
