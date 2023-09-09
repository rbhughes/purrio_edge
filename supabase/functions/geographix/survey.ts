import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

// ignores well_dir_proposed_srvy_station.gx_dls

const defineSQL = (filter) => {
  const D = "|&|";
  const N = "purrNULL";

  filter = filter ? filter : "";

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT * FROM (
    WITH w AS (
      SELECT
        uwi                        AS w_uwi,
        assigned_field             AS w_assigned_field,
        common_well_name           AS w_common_well_name,
        completion_date            AS w_completion_date,
        country                    AS w_country,
        county                     AS w_county,
        current_class              AS w_current_class,
        current_status             AS w_current_status,
        depth_datum                AS w_depth_datum,
        final_td                   AS w_final_td,
        ground_elev                AS w_ground_elev,
        kb_elev                    AS w_kb_elev,
        lease_name                 AS w_lease_name,
        operator                   AS w_operator,
        province_state             AS w_province_state,
        row_changed_date           AS w_row_changed_date,
        spud_date                  AS w_spud_date,
        surface_latitude           AS w_surface_latitude,
        surface_longitude          AS w_surface_longitude,
        td_form                    AS w_td_form,
        well_name                  AS w_well_name,
        well_number                AS w_well_number 
      FROM well
    ),
    ss AS (
      SELECT 
        uwi                 AS id_ss_uwi,
        survey_id           AS id_ss_survey_id,
        source              AS id_ss_source,
        'actual'            AS id_ss_kind,
        LIST(IFNULL(azimuth,              '${N}', CAST(azimuth AS VARCHAR)),                 '${D}' ORDER BY station_md)  AS ss_azimuth,
        LIST(IFNULL(azimuth_ouom,         '${N}', azimuth_ouom),                             '${D}' ORDER BY station_md)  AS ss_azimuth_ouom,
        LIST(IFNULL(ew_direction,         '${N}', ew_direction),                             '${D}' ORDER BY station_md)  AS ss_ew_direction,
        LIST(IFNULL(gx_closure,           '${N}', gx_closure),                               '${D}' ORDER BY station_md)  AS ss_gx_closure,
        LIST(IFNULL(gx_station_latitude,  '${N}', CAST(gx_station_latitude AS VARCHAR)),     '${D}' ORDER BY station_md)  AS ss_gx_station_latitude,
        LIST(IFNULL(gx_station_longitude, '${N}', CAST(gx_station_longitude AS VARCHAR)),    '${D}' ORDER BY station_md)  AS ss_gx_station_longitude,
        LIST(IFNULL(inclination,          '${N}', CAST(inclination AS VARCHAR)),             '${D}' ORDER BY station_md)  AS ss_inclination,
        LIST(IFNULL(inclination_ouom,     '${N}', inclination_ouom),                         '${D}' ORDER BY station_md)  AS ss_inclination_ouom,
        LIST(IFNULL(ns_direction,         '${N}', ns_direction),                             '${D}' ORDER BY station_md)  AS ss_ns_direction,
        LIST(IFNULL(row_changed_date,     '${N}', CAST(row_changed_date AS VARCHAR)),        '${D}' ORDER BY station_md)  AS ss_row_changed_date,
        LIST(IFNULL(source,               '${N}', source),                                   '${D}' ORDER BY station_md)  AS ss_source,
        LIST(IFNULL(station_id,           '${N}', station_id),                               '${D}' ORDER BY station_md)  AS ss_station_id,
        LIST(IFNULL(station_md,           '${N}', CAST(station_md AS VARCHAR)),              '${D}' ORDER BY station_md)  AS ss_station_md,
        LIST(IFNULL(station_md_ouom,      '${N}', station_md_ouom),                          '${D}' ORDER BY station_md)  AS ss_station_md_ouom,
        LIST(IFNULL(station_tvd,          '${N}', CAST(station_tvd AS VARCHAR)),             '${D}' ORDER BY station_md)  AS ss_station_tvd,
        LIST(IFNULL(station_tvd_ouom,     '${N}', station_tvd_ouom),                         '${D}' ORDER BY station_md)  AS ss_station_tvd_ouom,
        LIST(IFNULL(survey_id,            '${N}', survey_id),                                '${D}' ORDER BY station_md)  AS ss_survey_id,
        LIST(IFNULL(uwi,                  '${N}', uwi),                                      '${D}' ORDER BY station_md)  AS ss_uwi,
        LIST(IFNULL(x_offset,             '${N}', CAST(x_offset AS VARCHAR)),                '${D}' ORDER BY station_md)  AS ss_x_offset,
        LIST(IFNULL(x_offset_ouom,        '${N}', x_offset_ouom),                            '${D}' ORDER BY station_md)  AS ss_x_offset_ouom,
        LIST(IFNULL(y_offset,             '${N}', CAST(y_offset AS VARCHAR)),                '${D}' ORDER BY station_md)  AS ss_y_offset,
        LIST(IFNULL(y_offset_ouom,        '${N}', y_offset_ouom),                            '${D}' ORDER BY station_md)  AS ss_y_offset_ouom
      FROM well_dir_srvy_station
      GROUP BY uwi, survey_id, source
     UNION
      SELECT 
        uwi                 AS id_ss_uwi,
        survey_id           AS id_ss_survey_id,
        source              AS id_ss_source,
        'proposed'          AS id_ss_kind,
        LIST(IFNULL(azimuth,              '${N}', CAST(azimuth AS VARCHAR)),                 '${D}' ORDER BY station_md)  AS ss_azimuth,
        LIST(IFNULL(azimuth_ouom,         '${N}', azimuth_ouom),                             '${D}' ORDER BY station_md)  AS ss_azimuth_ouom,
        LIST(IFNULL(ew_direction,         '${N}', ew_direction),                             '${D}' ORDER BY station_md)  AS ss_ew_direction,
        LIST(IFNULL(gx_closure,           '${N}', gx_closure),                               '${D}' ORDER BY station_md)  AS ss_gx_closure,
        LIST(IFNULL(gx_station_latitude,  '${N}', CAST(gx_station_latitude AS VARCHAR)),     '${D}' ORDER BY station_md)  AS ss_gx_station_latitude,
        LIST(IFNULL(gx_station_longitude, '${N}', CAST(gx_station_longitude AS VARCHAR)),    '${D}' ORDER BY station_md)  AS ss_gx_station_longitude,
        LIST(IFNULL(inclination,          '${N}', CAST(inclination AS VARCHAR)),             '${D}' ORDER BY station_md)  AS ss_inclination,
        LIST(IFNULL(inclination_ouom,     '${N}', inclination_ouom),                         '${D}' ORDER BY station_md)  AS ss_inclination_ouom,
        LIST(IFNULL(ns_direction,         '${N}', ns_direction),                             '${D}' ORDER BY station_md)  AS ss_ns_direction,
        LIST(IFNULL(row_changed_date,     '${N}', CAST(row_changed_date AS VARCHAR)),        '${D}' ORDER BY station_md)  AS ss_row_changed_date,
        LIST(IFNULL(source,               '${N}', source),                                   '${D}' ORDER BY station_md)  AS ss_source,
        LIST(IFNULL(station_id,           '${N}', station_id),                               '${D}' ORDER BY station_md)  AS ss_station_id,
        LIST(IFNULL(station_md,           '${N}', CAST(station_md AS VARCHAR)),              '${D}' ORDER BY station_md)  AS ss_station_md,
        LIST(IFNULL(station_md_ouom,      '${N}', station_md_ouom),                          '${D}' ORDER BY station_md)  AS ss_station_md_ouom,
        LIST(IFNULL(station_tvd,          '${N}', CAST(station_tvd AS VARCHAR)),             '${D}' ORDER BY station_md)  AS ss_station_tvd,
        LIST(IFNULL(station_tvd_ouom,     '${N}', station_tvd_ouom),                         '${D}' ORDER BY station_md)  AS ss_station_tvd_ouom,
        LIST(IFNULL(survey_id,            '${N}', survey_id),                                '${D}' ORDER BY station_md)  AS ss_survey_id,
        LIST(IFNULL(uwi,                  '${N}', uwi),                                      '${D}' ORDER BY station_md)  AS ss_uwi,
        LIST(IFNULL(x_offset,             '${N}', CAST(x_offset AS VARCHAR)),                '${D}' ORDER BY station_md)  AS ss_x_offset,
        LIST(IFNULL(x_offset_ouom,        '${N}', x_offset_ouom),                            '${D}' ORDER BY station_md)  AS ss_x_offset_ouom,
        LIST(IFNULL(y_offset,             '${N}', CAST(y_offset AS VARCHAR)),                '${D}' ORDER BY station_md)  AS ss_y_offset,
        LIST(IFNULL(y_offset_ouom,        '${N}', y_offset_ouom),                            '${D}' ORDER BY station_md)  AS ss_y_offset_ouom
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
    )
    SELECT
      w.*,
      s.*,
      ss.*
    FROM ss
    JOIN s
      ON s.s_source = ss.id_ss_source
      AND s.s_survey_id = ss.id_ss_survey_id
      AND s.s_uwi = ss.id_ss_uwi
      AND s.s_kind = ss.id_ss_kind
    JOIN w ON w.w_uwi = s.s_uwi
    ) x`;

  const order = `ORDER BY w_uwi`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  //const fast_count = `SELECT COUNT(DISTINCT wellid) AS count FROM gx_well_curve`;

  return {
    select: select,
    count: count,
    order: order,
    where: where,
  };
};

const xformer = (args) => {
  let { func, key, typ, arg, obj } = args;

  const ensureType = (type: string, val: any) => {
    if (val == null) {
      return null;
    } else if (type === "object") {
      console.log("UNEXPECTED OBJECT TYPE! (needs xformer)", type);
      console.log(val);
      return null;
    } else if (type === "string") {
      //return decodeWin1252(val)
      return val.replace(/[\u0000-\u001F\u007F-\u009F]/g, "");
    } else if (type === "number") {
      // cuz blank strings (\t\r\n) evaluate to 0
      if (val.toString().replace(/\s/g, "") === "") {
        return null;
      }
      let n = Number(val);
      return isNaN(n) ? null : n;
    } else if (type === "date") {
      try {
        return new Date(val).toISOString();
      } catch (error) {
        return null;
      }
    } else {
      console.log("ENSURE TYPE SOMETHING ELSE (xformer)", type);
      return "XFORM ME";
    }
  };

  const D = "|&|";
  const N = "purrNULL";

  if (obj[key] == null) {
    return null;
  }

  switch (func) {
    case "delimited_array_with_nulls":
      return (() => {
        try {
          return obj[key]
            .split(D)
            .map((v) => (v === N ? null : ensureType(typ, v)));
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    case "blob_to_hex":
      return (() => {
        try {
          return Buffer.from(obj[key]).toString("hex");
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    default:
      return ensureType(typ, obj[key]);
  }
};

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },
  w_assigned_field: {
    ts_type: "string",
  },
  w_common_well_name: {
    ts_type: "string",
  },
  w_completion_date: {
    ts_type: "date",
  },
  w_country: {
    ts_type: "string",
  },
  w_county: {
    ts_type: "string",
  },
  w_current_class: {
    ts_type: "string",
  },
  w_current_status: {
    ts_type: "string",
  },
  w_depth_datum: {
    ts_type: "number",
  },
  w_final_td: {
    ts_type: "number",
  },
  w_ground_elev: {
    ts_type: "number",
  },
  w_kb_elev: {
    ts_type: "number",
  },
  w_lease_name: {
    ts_type: "string",
  },
  w_operator: {
    ts_type: "string",
  },
  w_province_state: {
    ts_type: "string",
  },
  w_row_changed_date: {
    ts_type: "date",
  },
  w_spud_date: {
    ts_type: "date",
  },
  w_surface_latitude: {
    ts_type: "number",
  },
  w_surface_longitude: {
    ts_type: "number",
  },
  w_td_form: {
    ts_type: "string",
  },
  w_well_name: {
    ts_type: "string",
  },
  w_well_number: {
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

  id_ss_uwi: {
    ts_type: "string",
  },
  id_ss_survey_id: {
    ts_type: "string",
  },
  id_ss_source: {
    ts_type: "string",
  },
  id_ss_kind: {
    ts_type: "string",
  },
  ss_azimuth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  ss_azimuth_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_ew_direction: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_gx_closure: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_gx_station_latitude: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  ss_gx_station_longitude: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  ss_inclination: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  ss_inclination_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_ns_direction: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  ss_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_station_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_station_md: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  ss_station_md_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_station_tvd: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  ss_station_tvd_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_survey_id: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_x_offset: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  ss_x_offset_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  ss_y_offset: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  ss_y_offset_ouom: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
};

const prefixes = {
  w_: "well",
  ss_: "well_dir_srvy_station",
  s_: "well_dir_srvy",
};

const global_id_keys = ["w_uwi"];

const well_id_keys = ["w_uwi"];

const pg_cols = ["id", "repo_id", "well_id", "geo_type", "tag", "doc"];

const default_chunk = 500;

///////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    default_chunk: default_chunk,
    global_id_keys: global_id_keys,
    pg_cols: pg_cols,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
