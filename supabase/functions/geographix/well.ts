import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT * FROM (
    WITH
    w AS (
      SELECT
        uwi                         AS w_uwi,
        abandonment_date            AS w_abandonment_date,
        assigned_field              AS w_assigned_field,
        base_node_id                AS w_base_node_id,
        bottom_hole_latitude        AS w_bottom_hole_latitude,
        bottom_hole_longitude       AS w_bottom_hole_longitude,
        casing_flange_elev          AS w_casing_flange_elev,
        casing_flange_elev_ouom     AS w_casing_flange_elev_ouom,
        common_well_name            AS w_common_well_name,
        completion_date             AS w_completion_date,
        confidential_date           AS w_confidential_date,
        confidential_depth          AS w_confidential_depth,
        confidential_depth_ouom     AS w_confidential_depth_ouom,
        confidential_form           AS w_confidential_form,
        confidential_type           AS w_confidential_type,
        country                     AS w_country,
        county                      AS w_county,
        current_class               AS w_current_class,
        current_status              AS w_current_status,
        current_status_date         AS w_current_status_date,
        depth_datum                 AS w_depth_datum,
        depth_datum_elev            AS w_depth_datum_elev,
        depth_datum_elev_ouom       AS w_depth_datum_elev_ouom,
        discovery_ind               AS w_discovery_ind,
        district                    AS w_district,
        drill_td                    AS w_drill_td,
        drill_td_ouom               AS w_drill_td_ouom,
        faulted_ind                 AS w_faulted_ind,
        final_drill_date            AS w_final_drill_date,
        final_td                    AS w_final_td,
        final_td_ouom               AS w_final_td_ouom,
        geodetic_datum_id           AS w_geodetic_datum_id,
        geographic_region           AS w_geographic_region,
        geologic_province           AS w_geologic_province,
        ggx_internal_status         AS w_ggx_internal_status,
        ground_elev                 AS w_ground_elev,
        ground_elev_ouom            AS w_ground_elev_ouom,
        ground_elev_type            AS w_ground_elev_type,
        gx_alternate_id             AS w_gx_alternate_id,
        gx_bottom_hole_ew_direction AS w_gx_bottom_hole_ew_direction,
        gx_bottom_hole_ns_direction AS w_gx_bottom_hole_ns_direction,
        gx_bottom_hole_tvd          AS w_gx_bottom_hole_tvd,
        gx_bottom_hole_x_offset     AS w_gx_bottom_hole_x_offset,
        gx_bottom_hole_y_offset     AS w_gx_bottom_hole_y_offset,
        gx_dev_well_blob            AS w_gx_dev_well_blob,
        gx_formid                   AS w_gx_formid,
        gx_formid_alias             AS w_gx_formid_alias,
        gx_legal_string             AS w_gx_legal_string,
        gx_location_string          AS w_gx_location_string,
        gx_mag_declination          AS w_gx_mag_declination,
        gx_old_id                   AS w_gx_old_id,
        gx_oldest_form_alias        AS w_gx_oldest_form_alias,
        gx_original_units           AS w_gx_original_units,
        gx_percent_allocation       AS w_gx_percent_allocation,
        gx_permit_date              AS w_gx_permit_date,
        gx_point                    AS w_gx_point,
        gx_proposed_flag            AS w_gx_proposed_flag,
        gx_remarks                  AS w_gx_remarks,
        gx_rigfloor_elev            AS w_gx_rigfloor_elev,
        gx_td_form_alias            AS w_gx_td_form_alias,
        gx_user_date                AS w_gx_user_date,
        gx_user1                    AS w_gx_user1,
        gx_user2                    AS w_gx_user2,
        gx_wsn                      AS w_gx_wsn,
        initial_class               AS w_initial_class,
        kb_elev                     AS w_kb_elev,
        kb_elev_ouom                AS w_kb_elev_ouom,
        lease_name                  AS w_lease_name,
        lease_number                AS w_lease_number,
        legal_survey_type           AS w_legal_survey_type,
        log_td                      AS w_log_td,
        log_td_ouom                 AS w_log_td_ouom,
        max_tvd                     AS w_max_tvd,
        max_tvd_ouom                AS w_max_tvd_ouom,
        net_pay                     AS w_net_pay,
        net_pay_ouom                AS w_net_pay_ouom,
        oldest_form                 AS w_oldest_form,
        oldest_form_age             AS w_oldest_form_age,
        operator                    AS w_operator,
        original_operator           AS w_original_operator,
        parent_relationship_type    AS w_parent_relationship_type,
        parent_uwi                  AS w_parent_uwi,
        platform_id                 AS w_platform_id,
        platform_source             AS w_platform_source,
        plot_name                   AS w_plot_name,
        plot_symbol                 AS w_plot_symbol,
        plugback_depth              AS w_plugback_depth,
        plugback_depth_ouom         AS w_plugback_depth_ouom,
        primary_source              AS w_primary_source,
        profile_type                AS w_profile_type,
        province_state              AS w_province_state,
        regulatory_agency           AS w_regulatory_agency,
        rig_release_date            AS w_rig_release_date,
        row_changed_date            AS w_row_changed_date,
        source_document             AS w_source_document,
        spud_date                   AS w_spud_date,
        surface_latitude            AS w_surface_latitude,
        surface_longitude           AS w_surface_longitude,
        surface_node_id             AS w_surface_node_id,
        tax_credit_code             AS w_tax_credit_code,
        td_form                     AS w_td_form,
        td_form_age                 AS w_td_form_age,
        water_depth                 AS w_water_depth,
        water_depth_datum           AS w_water_depth_datum,
        water_depth_ouom            AS w_water_depth_ouom,
        well_govt_id                AS w_well_govt_id,
        well_intersect_md           AS w_well_intersect_md,
        well_name                   AS w_well_name,
        well_number                 AS w_well_number,
        whipstock_depth             AS w_whipstock_depth,
        whipstock_depth_ouom        AS w_whipstock_depth_ouom,
        ggx_internal_status         AS fk_internal_status 
      FROM well
    ),
    co AS (
      SELECT
        uwi                         AS co_uwi,
        location_type               AS co_location_type,
        bh_flag                     AS co_bh_flag,
        source                      AS co_source,
        congress_range              AS co_congress_range,
        congress_sect_suffix        AS co_congress_sect_suffix,
        ew_direction                AS co_ew_direction,
        congress_section            AS co_congress_section,
        congress_sect_type          AS co_congress_sect_type,
        congress_township           AS co_congress_township,
        ns_direction                AS co_ns_direction,
        congress_twp_name           AS co_congress_twp_name,
        principal_meridian          AS co_principal_meridian,
        ns_footage                  AS co_ns_footage,
        ns_start_line               AS co_ns_start_line,
        ew_footage                  AS co_ew_footage,
        ew_start_line               AS co_ew_start_line,
        footage_origin              AS co_footage_origin,
        spot_code                   AS co_spot_code,
        remark                      AS co_remark,
        ns_footage_ouom             AS co_ns_footage_ouom,
        ew_footage_ouom             AS co_ew_footage_ouom,
        row_changed_date            AS co_row_changed_date,
        polygonid                   AS co_polygonid,
        county                      AS co_county,
        country                     AS co_country,
        province_state              AS co_province_state
      FROM legal_congress_loc
    )
    SELECT
      w.*,
      co.*
    FROM  w
    LEFT JOIN co ON
      co.co_uwi = w.w_uwi
    ) x`;

  const order = `ORDER BY w_uwi`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  //const fast_count = `SELECT COUNT(DISTINCT uwi) AS count FROM well`;

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

  if (obj[key] == null) {
    return null;
  }

  switch (func) {
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
  w_abandonment_date: {
    ts_type: "date",
  },
  w_assigned_field: {
    ts_type: "string",
  },
  w_base_node_id: {
    ts_type: "string",
  },
  w_bottom_hole_latitude: {
    ts_type: "number",
  },
  w_bottom_hole_longitude: {
    ts_type: "number",
  },
  w_casing_flange_elev: {
    ts_type: "number",
  },
  w_casing_flange_elev_ouom: {
    ts_type: "string",
  },
  w_common_well_name: {
    ts_type: "string",
  },
  w_completion_date: {
    ts_type: "date",
  },
  w_confidential_date: {
    ts_type: "date",
  },
  w_confidential_depth: {
    ts_type: "number",
  },
  w_confidential_depth_ouom: {
    ts_type: "string",
  },
  w_confidential_form: {
    ts_type: "string",
  },
  w_confidential_type: {
    ts_type: "string",
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
  w_current_status_date: {
    ts_type: "date",
  },
  w_depth_datum: {
    ts_type: "string",
  },
  w_depth_datum_elev: {
    ts_type: "number",
  },
  w_depth_datum_elev_ouom: {
    ts_type: "string",
  },
  w_discovery_ind: {
    ts_type: "string",
  },
  w_district: {
    ts_type: "string",
  },
  w_drill_td: {
    ts_type: "number",
  },
  w_drill_td_ouom: {
    ts_type: "string",
  },
  w_faulted_ind: {
    ts_type: "string",
  },
  w_final_drill_date: {
    ts_type: "date",
  },
  w_final_td: {
    ts_type: "number",
  },
  w_final_td_ouom: {
    ts_type: "string",
  },
  w_geodetic_datum_id: {
    ts_type: "string",
  },
  w_geographic_region: {
    ts_type: "string",
  },
  w_geologic_province: {
    ts_type: "string",
  },
  w_ggx_internal_status: {
    ts_type: "string",
  },
  w_ground_elev: {
    ts_type: "number",
  },
  w_ground_elev_ouom: {
    ts_type: "string",
  },
  w_ground_elev_type: {
    ts_type: "string",
  },
  w_gx_alternate_id: {
    ts_type: "string",
  },
  w_gx_bottom_hole_ew_direction: {
    ts_type: "string",
  },
  w_gx_bottom_hole_ns_direction: {
    ts_type: "string",
  },
  w_gx_bottom_hole_tvd: {
    ts_type: "number",
  },
  w_gx_bottom_hole_x_offset: {
    ts_type: "number",
  },
  w_gx_bottom_hole_y_offset: {
    ts_type: "number",
  },
  w_gx_dev_well_blob: {
    ts_type: "string",
    xform: "blob_to_hex",
  },
  w_gx_formid: {
    ts_type: "string",
  },
  w_gx_formid_alias: {
    ts_type: "string",
  },
  w_gx_legal_string: {
    ts_type: "string",
  },
  w_gx_location_string: {
    ts_type: "string",
  },
  w_gx_mag_declination: {
    ts_type: "number",
  },
  w_gx_old_id: {
    ts_type: "string",
  },
  w_gx_oldest_form_alias: {
    ts_type: "string",
  },
  w_gx_original_units: {
    ts_type: "string",
  },
  w_gx_percent_allocation: {
    ts_type: "number",
  },
  w_gx_permit_date: {
    ts_type: "date",
  },
  w_gx_point: {
    ts_type: "string",
  },
  w_gx_proposed_flag: {
    ts_type: "number",
  },
  w_gx_remarks: {
    ts_type: "string",
  },
  w_gx_rigfloor_elev: {
    ts_type: "number",
  },
  w_gx_td_form_alias: {
    ts_type: "string",
  },
  w_gx_user1: {
    ts_type: "string",
  },
  w_gx_user2: {
    ts_type: "string",
  },
  w_gx_user_date: {
    ts_type: "date",
  },
  w_gx_wsn: {
    ts_type: "number",
  },
  w_initial_class: {
    ts_type: "string",
  },
  w_kb_elev: {
    ts_type: "number",
  },
  w_kb_elev_ouom: {
    ts_type: "string",
  },
  w_lease_name: {
    ts_type: "string",
  },
  w_lease_number: {
    ts_type: "string",
  },
  w_legal_survey_type: {
    ts_type: "string",
  },
  w_log_td: {
    ts_type: "number",
  },
  w_log_td_ouom: {
    ts_type: "string",
  },
  w_max_tvd: {
    ts_type: "number",
  },
  w_max_tvd_ouom: {
    ts_type: "string",
  },
  w_net_pay: {
    ts_type: "number",
  },
  w_net_pay_ouom: {
    ts_type: "string",
  },
  w_oldest_form: {
    ts_type: "string",
  },
  w_oldest_form_age: {
    ts_type: "string",
  },
  w_operator: {
    ts_type: "string",
  },
  w_original_operator: {
    ts_type: "string",
  },
  w_parent_relationship_type: {
    ts_type: "string",
  },
  w_parent_uwi: {
    ts_type: "string",
  },
  w_platform_id: {
    ts_type: "string",
  },
  w_platform_source: {
    ts_type: "string",
  },
  w_plot_name: {
    ts_type: "string",
  },
  w_plot_symbol: {
    ts_type: "string",
  },
  w_plugback_depth: {
    ts_type: "number",
  },
  w_plugback_depth_ouom: {
    ts_type: "string",
  },
  w_primary_source: {
    ts_type: "string",
  },
  w_profile_type: {
    ts_type: "string",
  },
  w_province_state: {
    ts_type: "string",
  },
  w_regulatory_agency: {
    ts_type: "string",
  },
  w_rig_release_date: {
    ts_type: "date",
  },
  w_row_changed_date: {
    ts_type: "date",
  },
  w_source_document: {
    ts_type: "string",
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
  w_surface_node_id: {
    ts_type: "string",
  },
  w_tax_credit_code: {
    ts_type: "string",
  },
  w_td_form: {
    ts_type: "string",
  },
  w_td_form_age: {
    ts_type: "string",
  },
  w_uwi: {
    ts_type: "string",
  },
  w_water_depth: {
    ts_type: "number",
  },
  w_water_depth_datum: {
    ts_type: "string",
  },
  w_water_depth_ouom: {
    ts_type: "string",
  },
  w_well_govt_id: {
    ts_type: "string",
  },
  w_well_intersect_md: {
    ts_type: "number",
  },
  w_well_name: {
    ts_type: "string",
  },
  w_well_number: {
    ts_type: "string",
  },
  w_whipstock_depth: {
    ts_type: "number",
  },
  w_whipstock_depth_ouom: {
    ts_type: "string",
  },

  // R_INTERNAL_STATUS

  fk_internal_status: {
    ts_type: "string",
  },

  // LEGAL_CARTER_LOC

  // LEGAL_CONGRESS_LOC

  co_bh_flag: {
    ts_type: "number",
  },
  co_congress_range: {
    ts_type: "number",
  },
  co_congress_sect_suffix: {
    ts_type: "string",
  },
  co_congress_sect_type: {
    ts_type: "string",
  },
  co_congress_section: {
    ts_type: "number",
  },
  co_congress_township: {
    ts_type: "number",
  },
  co_congress_twp_name: {
    ts_type: "string",
  },
  co_country: {
    ts_type: "string",
  },
  co_county: {
    ts_type: "string",
  },
  co_ew_direction: {
    ts_type: "string",
  },
  co_ew_footage: {
    ts_type: "number",
  },
  co_ew_footage_ouom: {
    ts_type: "string",
  },
  co_ew_start_line: {
    ts_type: "string",
  },
  co_footage_origin: {
    ts_type: "string",
  },
  co_location_type: {
    ts_type: "string",
  },
  co_ns_direction: {
    ts_type: "string",
  },
  co_ns_footage: {
    ts_type: "number",
  },
  co_ns_footage_ouom: {
    ts_type: "string",
  },
  co_ns_start_line: {
    ts_type: "string",
  },
  co_polygonid: {
    ts_type: "number",
  },
  co_principal_meridian: {
    ts_type: "string",
  },
  co_province_state: {
    ts_type: "string",
  },
  co_remark: {
    ts_type: "string",
  },
  co_row_changed_date: {
    ts_type: "date",
  },
  co_source: {
    ts_type: "string",
  },
  co_spot_code: {
    ts_type: "string",
  },
  co_uwi: {
    ts_type: "string",
  },

  // LEGAL_DLS_LOC
  // LEGAL_FPS_LOC
  // LEGAL_NE_LOC
  // LEGAL_NORTH_SEA_LOC
  // LEGAL_NTS_LOC
  // LEGAL_OFFSHORE_LOC
  // LEGAL_OHIO_LOC
  // LEGAL_TEXAS_LOC
};

const prefixes = {
  w_: "well",
  co_: "legal_congress_loc",
};

const global_id_keys = ["w_uwi"];

const well_id_keys = ["w_uwi"];

const pg_cols = ["id", "repo_id", "well_id", "geo_type", "tag", "doc"];

const default_chunk = 1000;

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
