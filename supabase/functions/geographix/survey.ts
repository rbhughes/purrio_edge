import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

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
        ss.uwi              AS id_ss_uwi,
        ss.survey_id        AS id_ss_survey_id,
        ss.source           AS id_ss_source,
        'actual'            AS id_ss_kind,
        LIST(ss.azimuth,              '${D}' ORDER BY station_md)  AS ss_azimuth,
        LIST(ss.azimuth_ouom,         '${D}' ORDER BY station_md)  AS ss_azimuth_ouom,
        LIST(ss.ew_direction,         '${D}' ORDER BY station_md)  AS ss_ew_direction,
        LIST(ss.gx_closure,           '${D}' ORDER BY station_md)  AS ss_gx_closure,
        LIST(null,                    '${D}' ORDER BY station_md)  AS ss_gx_dls,
        LIST(ss.gx_station_latitude,  '${D}' ORDER BY station_md)  AS ss_gx_station_latitude,
        LIST(ss.gx_station_longitude, '${D}' ORDER BY station_md)  AS ss_gx_station_longitude,
        LIST(ss.inclination,          '${D}' ORDER BY station_md)  AS ss_inclination,
        LIST(ss.inclination_ouom,     '${D}' ORDER BY station_md)  AS ss_inclination_ouom,
        LIST(ss.ns_direction,         '${D}' ORDER BY station_md)  AS ss_ns_direction,
        LIST(ss.row_changed_date,     '${D}' ORDER BY station_md)  AS ss_row_changed_date,
        LIST(ss.source,               '${D}' ORDER BY station_md)  AS ss_source,
        LIST(ss.station_id,           '${D}' ORDER BY station_md)  AS ss_station_id,
        LIST(ss.station_md,           '${D}' ORDER BY station_md)  AS ss_station_md,
        LIST(ss.station_md_ouom,      '${D}' ORDER BY station_md)  AS ss_station_md_ouom,
        LIST(ss.station_tvd,          '${D}' ORDER BY station_md)  AS ss_station_tvd,
        LIST(ss.station_tvd_ouom,     '${D}' ORDER BY station_md)  AS ss_station_tvd_ouom,
        LIST(ss.survey_id,            '${D}' ORDER BY station_md)  AS ss_survey_id,
        LIST(ss.uwi,                  '${D}' ORDER BY station_md)  AS ss_uwi,
        LIST(ss.x_offset,             '${D}' ORDER BY station_md)  AS ss_x_offset,
        LIST(ss.x_offset_ouom,        '${D}' ORDER BY station_md)  AS ss_x_offset_ouom,
        LIST(ss.y_offset,             '${D}' ORDER BY station_md)  AS ss_y_offset,
        LIST(ss.y_offset_ouom,        '${D}' ORDER BY station_md)  AS ss_y_offset_ouom
      FROM well_dir_srvy_station ss
      GROUP BY uwi, survey_id, source
      UNION
      SELECT 
        ss.uwi              AS id_ss_uwi,
        ss.survey_id        AS id_ss_survey_id,
        ss.source           AS id_ss_source,
        'proposed'          AS id_ss_kind,
        LIST(ss.azimuth,              '${D}' ORDER BY station_md)  AS ss_azimuth,
        LIST(ss.azimuth_ouom,         '${D}' ORDER BY station_md)  AS ss_azimuth_ouom,
        LIST(ss.ew_direction,         '${D}' ORDER BY station_md)  AS ss_ew_direction,
        LIST(ss.gx_closure,           '${D}' ORDER BY station_md)  AS ss_gx_closure,
        LIST(null,                    '${D}' ORDER BY station_md)  AS ss_gx_dls,
        LIST(ss.gx_station_latitude,  '${D}' ORDER BY station_md)  AS ss_gx_station_latitude,
        LIST(ss.gx_station_longitude, '${D}' ORDER BY station_md)  AS ss_gx_station_longitude,
        LIST(ss.inclination,          '${D}' ORDER BY station_md)  AS ss_inclination,
        LIST(ss.inclination_ouom,     '${D}' ORDER BY station_md)  AS ss_inclination_ouom,
        LIST(ss.ns_direction,         '${D}' ORDER BY station_md)  AS ss_ns_direction,
        LIST(ss.row_changed_date,     '${D}' ORDER BY station_md)  AS ss_row_changed_date,
        LIST(ss.source,               '${D}' ORDER BY station_md)  AS ss_source,
        LIST(ss.station_id,           '${D}' ORDER BY station_md)  AS ss_station_id,
        LIST(ss.station_md,           '${D}' ORDER BY station_md)  AS ss_station_md,
        LIST(ss.station_md_ouom,      '${D}' ORDER BY station_md)  AS ss_station_md_ouom,
        LIST(ss.station_tvd,          '${D}' ORDER BY station_md)  AS ss_station_tvd,
        LIST(ss.station_tvd_ouom,     '${D}' ORDER BY station_md)  AS ss_station_tvd_ouom,
        LIST(ss.survey_id,            '${D}' ORDER BY station_md)  AS ss_survey_id,
        LIST(ss.uwi,                  '${D}' ORDER BY station_md)  AS ss_uwi,
        LIST(ss.x_offset,             '${D}' ORDER BY station_md)  AS ss_x_offset,
        LIST(ss.x_offset_ouom,        '${D}' ORDER BY station_md)  AS ss_x_offset_ouom,
        LIST(ss.y_offset,             '${D}' ORDER BY station_md)  AS ss_y_offset,
        LIST(ss.y_offset_ouom,        '${D}' ORDER BY station_md)  AS ss_y_offset_ouom
      FROM well_dir_proposed_srvy_station ss
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
    default:
      return ensureType(typ, obj[key]);
  }
};

const xforms = {
  // WELL

  w_uwi: {
    json_name: "uwi",
    qb_name: "WELL.uwi",
    parent: "well",
    ts_type: "string",
  },
  w_assigned_field: {
    json_name: "assigned_field",
    qb_name: "WELL.assigned_field",
    parent: "well",
    ts_type: "string",
  },
  w_common_well_name: {
    json_name: "common_well_name",
    qb_name: "WELL.common_well_name",
    parent: "well",
    ts_type: "string",
  },
  w_completion_date: {
    json_name: "completion_date",
    qb_name: "WELL.completion_date",
    parent: "well",
    ts_type: "date",
  },
  w_country: {
    json_name: "country",
    qb_name: "WELL.country",
    parent: "well",
    ts_type: "string",
  },
  w_county: {
    json_name: "county",
    qb_name: "WELL.county",
    parent: "well",
    ts_type: "string",
  },
  w_current_class: {
    json_name: "current_class",
    qb_name: "WELL.current_class",
    parent: "well",
    ts_type: "string",
  },
  w_current_status: {
    json_name: "current_status",
    qb_name: "WELL.current_status",
    parent: "well",
    ts_type: "string",
  },
  w_depth_datum: {
    json_name: "depth_datum",
    qb_name: "WELL.depth_datum",
    parent: "well",
    ts_type: "number",
  },
  w_final_td: {
    json_name: "final_td",
    qb_name: "WELL.final_td",
    parent: "well",
    ts_type: "number",
  },
  w_ground_elev: {
    json_name: "ground_elev",
    qb_name: "WELL.ground_elev",
    parent: "well",
    ts_type: "number",
  },
  w_kb_elev: {
    json_name: "kb_elev",
    qb_name: "WELL.kb_elev",
    parent: "well",
    ts_type: "number",
  },
  w_lease_name: {
    json_name: "lease_name",
    qb_name: "WELL.lease_name",
    parent: "well",
    ts_type: "string",
  },
  w_operator: {
    json_name: "operator",
    qb_name: "WELL.operator",
    parent: "well",
    ts_type: "string",
  },
  w_province_state: {
    json_name: "province_state",
    qb_name: "WELL.province_state",
    parent: "well",
    ts_type: "string",
  },
  w_row_changed_date: {
    json_name: "row_changed_date",
    qb_name: "WELL.row_changed_date",
    parent: "well",
    ts_type: "date",
  },
  w_spud_date: {
    json_name: "spud_date",
    qb_name: "WELL.spud_date",
    parent: "well",
    ts_type: "date",
  },
  w_surface_latitude: {
    json_name: "surface_latitude",
    qb_name: "WELL.surface_latitude",
    parent: "well",
    ts_type: "number",
  },
  w_surface_longitude: {
    json_name: "surface_longitude",
    qb_name: "WELL.surface_longitude",
    parent: "well",
    ts_type: "number",
  },
  w_td_form: {
    json_name: "td_form",
    qb_name: "WELL.td_form",
    parent: "well",
    ts_type: "string",
  },
  w_well_name: {
    json_name: "well_name",
    qb_name: "WELL.well_name",
    parent: "well",
    ts_type: "string",
  },
  w_well_number: {
    json_name: "well_number",
    qb_name: "WELL.well_number",
    parent: "well",
    ts_type: "string",
  },

  // WELL_DIR_SRVY + WELL_DIR_PROPOSED_SRVY

  s_kind: {
    json_name: "kind",
    qb_name: "kind (actual or proposed)",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_azimuth_north_type: {
    json_name: "azimuth_north_type",
    qb_name: "(survey) azimuth_north_type",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_base_depth: {
    json_name: "base_depth",
    qb_name: "(survey) base_depth",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_base_depth_ouom: {
    json_name: "base_depth_ouom",
    qb_name: "(survey) base_depth_ouom",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_calculation_required: {
    json_name: "calculation_required",
    qb_name: "(survey) calculation_required",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_compute_method: {
    json_name: "compute_method",
    qb_name: "(survey) compute_method",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_ew_magnetic_declination: {
    json_name: "ew_magnetic_declination",
    qb_name: "(survey) ew_magnetic_declination",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_grid_system_id: {
    json_name: "grid_system_id",
    qb_name: "(survey) grid_system_id",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_gx_active: {
    json_name: "gx_active",
    qb_name: "(survey) gx_active",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_base_e_w_offset: {
    json_name: "gx_base_e_w_offset",
    qb_name: "(survey) gx_base_e_w_offset",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_base_latitude: {
    json_name: "gx_base_latitude",
    qb_name: "(survey) gx_base_latitude",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_base_location_string: {
    json_name: "gx_base_location_string",
    qb_name: "(survey) gx_base_location_string",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_gx_base_longitude: {
    json_name: "gx_base_longitude",
    qb_name: "(survey) gx_base_longitude",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_base_n_s_offset: {
    json_name: "gx_base_n_s_offset",
    qb_name: "(survey) gx_base_n_s_offset",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_base_tvd: {
    json_name: "gx_base_tvd",
    qb_name: "(survey) gx_base_tvd",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_closure: {
    json_name: "gx_closure",
    qb_name: "(survey) gx_closure",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_footage: {
    json_name: "gx_footage",
    qb_name: "(survey) gx_footage",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_gx_kop_e_w_offset: {
    json_name: "gx_kop_e_w_offset",
    qb_name: "(survey) gx_kop_e_w_offset",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_kop_latitude: {
    json_name: "gx_kop_latitude",
    qb_name: "(survey) gx_kop_latitude",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_kop_longitude: {
    json_name: "gx_kop_longitude",
    qb_name: "(survey) gx_kop_longitude",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_kop_md: {
    json_name: "gx_kop_md",
    qb_name: "(survey) gx_kop_md",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_kop_n_s_offset: {
    json_name: "gx_kop_n_s_offset",
    qb_name: "(survey) gx_kop_n_s_offset",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_kop_tvd: {
    json_name: "gx_kop_tvd",
    qb_name: "(survey) gx_kop_tvd",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_lp_e_w_offset: {
    json_name: "gx_lp_e_w_offset",
    qb_name: "(survey) gx_lp_e_w_offset",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_lp_n_s_offset: {
    json_name: "gx_lp_n_s_offset",
    qb_name: "(survey) gx_lp_n_s_offset",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_lp_tvd: {
    json_name: "gx_lp_tvd",
    qb_name: "(survey) gx_lp_tvd",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_gx_proposed_well_blob: {
    json_name: "gx_proposed_well_blob",
    qb_name: false,
    parent: "wds_or_wdps",
    ts_type: "string",
    xform: "blob_to_string",
  },
  s_gx_scenario_name: {
    json_name: "gx_scenario_name",
    qb_name: "(survey) gx_scenario_name",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_magnetic_declination: {
    json_name: "magnetic_declination",
    qb_name: "(survey) magnetic_declination",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_north_reference: {
    json_name: "north_reference",
    qb_name: "(survey) north_reference",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_offset_north_type: {
    json_name: "offset_north_type",
    qb_name: "(survey) offset_north_type",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_record_mode: {
    json_name: "record_mode",
    qb_name: "(survey) record_mode",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_remark: {
    json_name: "remark",
    qb_name: "(survey) remark",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_row_changed_date: {
    json_name: "row_changed_date",
    qb_name: "(survey) row_changed_date",
    parent: "wds_or_wdps",
    ts_type: "date",
  },
  s_source: {
    json_name: "source",
    qb_name: "(survey) source",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_source_document: {
    json_name: "source_document",
    qb_name: "(survey) source_document",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_survey_company: {
    json_name: "survey_company",
    qb_name: "(survey) survey_company",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_survey_date: {
    json_name: "survey_date",
    qb_name: "(survey) survey_date",
    parent: "wds_or_wdps",
    ts_type: "date",
  },
  s_survey_id: {
    json_name: "survey_id",
    qb_name: "(survey) survey_id",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_survey_quality: {
    json_name: "survey_quality",
    qb_name: "(survey) survey_quality",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_survey_type: {
    json_name: "survey_type",
    qb_name: "(survey) survey_type",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_top_depth: {
    json_name: "top_depth",
    qb_name: "(survey) top_depth",
    parent: "wds_or_wdps",
    ts_type: "number",
  },
  s_top_depth_ouom: {
    json_name: "top_depth_ouom",
    qb_name: "(survey) top_depth_ouom",
    parent: "wds_or_wdps",
    ts_type: "string",
  },
  s_uwi: {
    json_name: "uwi",
    qb_name: "(survey) uwi",
    parent: "wds_or_wdps",
    ts_type: "string",
  },

  // WELL_DIR_SRVY_STATION + WELL_DIR_PROPOSED_SRVY_STATION

  id_ss_uwi: {
    json_name: false,
    qb_name: false,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  id_ss_survey_id: {
    json_name: false,
    qb_name: false,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  id_ss_source: {
    json_name: false,
    qb_name: false,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  id_ss_kind: {
    json_name: "kind",
    qb_name: false,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_azimuth: {
    json_name: "azimuth",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "number",
  },
  ss_azimuth_ouom: {
    json_name: "azimuth_ouom",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_ew_direction: {
    json_name: "ew_direction",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_gx_closure: {
    json_name: "gx_closure",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_gx_dls: {
    json_name: "gx_dls",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_gx_station_latitude: {
    json_name: "gx_station_latitude",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "number",
  },
  ss_gx_station_longitude: {
    json_name: "gx_station_longitude",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "number",
  },
  ss_inclination: {
    json_name: "inclination",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "number",
  },
  ss_inclination_ouom: {
    json_name: "inclination_ouom",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_ns_direction: {
    json_name: "ns_direction",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_row_changed_date: {
    json_name: "row_changed_date",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "date",
  },
  ss_source: {
    json_name: "source",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_station_id: {
    json_name: "station_id",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_station_md: {
    json_name: "station_md",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "number",
  },
  ss_station_md_ouom: {
    json_name: "station_md_ouom",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_station_tvd: {
    json_name: "station_tvd",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "number",
  },
  ss_station_tvd_ouom: {
    json_name: "station_tvd_ouom",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_survey_id: {
    json_name: "survey_id",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_uwi: {
    json_name: "uwi",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_x_offset: {
    json_name: "x_offset",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "number",
  },
  ss_x_offset_ouom: {
    json_name: "x_offset_ouom",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
  ss_y_offset: {
    json_name: "y_offset",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "number",
  },
  ss_y_offset_ouom: {
    json_name: "y_offset_ouom",
    qb_name: false,
    delimiter: D,
    parent: "wdss_or_wdpss",
    ts_type: "string",
  },
};

const prefixes = {
  w_: "well",
  m_: "gx_pden_vol_sum_by_month",
  p_: "well_cumulative_production",
  r_: "r_source",
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
