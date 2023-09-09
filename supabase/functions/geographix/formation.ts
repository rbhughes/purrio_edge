import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

// ignored:
// fault.fault_id
// r_source.source

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT * FROM (
    WITH w AS (
      SELECT
        uwi                       AS w_uwi,
        assigned_field            AS w_assigned_field,
        common_well_name          AS w_common_well_name,
        completion_date           AS w_completion_date,
        country                   AS w_country,
        county                    AS w_county,
        current_class             AS w_current_class,
        current_status            AS w_current_status,
        depth_datum               AS w_depth_datum,
        final_td                  AS w_final_td,
        ground_elev               AS w_ground_elev,
        kb_elev                   AS w_kb_elev,
        lease_name                AS w_lease_name,
        operator                  AS w_operator,
        province_state            AS w_province_state,
        row_changed_date          AS w_row_changed_date,
        spud_date                 AS w_spud_date,
        surface_latitude          AS w_surface_latitude,
        surface_longitude         AS w_surface_longitude,
        td_form                   AS w_td_form,
        well_name                 AS w_well_name,
        well_number               AS w_well_number        
      FROM well
    )
    SELECT 
      w.*,
      f.dominant_lithology        AS f_dominant_lithology,
      f.fault_name                AS f_fault_name,
      f.faulted_ind               AS f_faulted_ind,
      f.form_depth                AS f_form_depth,
      f.form_id                   AS f_form_id,
      f.form_obs_no               AS f_form_obs_no,
      f.form_tvd                  AS f_form_tvd,
      f.gap_thickness             AS f_gap_thickness,
      f.gx_base_subsea            AS f_gx_base_subsea,
      f.gx_dip                    AS f_gx_dip,
      f.gx_dip_azimuth            AS f_gx_dip_azimuth,
      f.gx_eroded_ind             AS f_gx_eroded_ind,
      f.gx_exten_id               AS f_gx_exten_id,
      f.gx_form_base_depth        AS f_gx_form_base_depth,
      f.gx_form_base_tvd          AS f_gx_form_base_tvd,
      f.gx_form_depth_datum       AS f_gx_form_depth_datum,
      f.gx_form_id_alias          AS f_gx_form_id_alias,
      f.gx_form_top_depth         AS f_gx_form_top_depth,
      f.gx_form_top_tvd           AS f_gx_form_top_tvd,
      f.gx_form_x_coordinate      AS f_gx_form_x_coordinate,
      f.gx_form_y_coordinate      AS f_gx_form_y_coordinate,
      f.gx_gross_thickness        AS f_gx_gross_thickness,
      f.gx_internal_no            AS f_gx_internal_no,
      f.gx_net_thickness          AS f_gx_net_thickness,
      f.gx_porosity               AS f_gx_porosity,
      f.gx_show                   AS f_gx_show,
      f.gx_strat_column           AS f_gx_strat_column,
      f.gx_top_subsea             AS f_gx_top_subsea,
      f.gx_true_strat_thickness   AS f_gx_true_strat_thickness,
      f.gx_true_vert_thickness    AS f_gx_true_vert_thickness,
      f.gx_user1                  AS f_gx_user1,
      f.gx_user2                  AS f_gx_user2,
      f.gx_user3                  AS f_gx_user3,
      f.gx_user4                  AS f_gx_user4,
      f.gx_user5                  AS f_gx_user5,
      f.gx_user6                  AS f_gx_user6,
      f.gx_vendor_no              AS f_gx_vendor_no,
      f.gx_wellbore_angle         AS f_gx_wellbore_angle,
      f.gx_wellbore_azimuth       AS f_gx_wellbore_azimuth,
      f.percent_thickness         AS f_percent_thickness,
      f.pick_location             AS f_pick_location,
      f.pick_qualifier            AS f_pick_qualifier,
      f.pick_quality              AS f_pick_quality,
      f.pick_type                 AS f_pick_type,
      f.public                    AS f_public,
      f.public                    AS m_public,
      f.remark                    AS f_remark,
      f.row_changed_date          AS f_row_changed_date,
      f.source                    AS f_source,
      f.unc_fault_obs_no          AS f_unc_fault_obs_no,
      f.unc_fault_source          AS f_unc_fault_source,
      f.unconformity_name         AS f_unconformity_name,
      f.unconformity_name         AS m_unconformity_name,
      f.uwi                       AS f_uwi
    FROM well_formation f
    JOIN w ON
      f.uwi = w.w_uwi AND coalesce(f.gx_form_top_depth, f.gx_form_base_depth) IS NOT NULL
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

  // WELL_FORMATION

  f_dominant_lithology: {
    ts_type: "string",
  },
  f_fault_name: {
    ts_type: "string",
  },
  f_faulted_ind: {
    ts_type: "number",
  },
  f_form_depth: {
    ts_type: "number",
  },
  f_form_id: {
    ts_type: "string",
  },
  f_form_obs_no: {
    ts_type: "number",
  },
  f_form_tvd: {
    ts_type: "number",
  },
  f_gap_thickness: {
    ts_type: "number",
  },
  f_gx_base_subsea: {
    ts_type: "number",
  },
  f_gx_dip: {
    ts_type: "string",
  },
  f_gx_dip_azimuth: {
    ts_type: "number",
  },
  f_gx_eroded_ind: {
    ts_type: "number",
  },
  f_gx_exten_id: {
    ts_type: "string",
  },
  f_gx_form_base_depth: {
    ts_type: "number",
  },
  f_gx_form_base_tvd: {
    ts_type: "number",
  },
  f_gx_form_depth_datum: {
    ts_type: "number",
  },
  f_gx_form_id_alias: {
    ts_type: "string",
  },
  f_gx_form_top_depth: {
    ts_type: "number",
  },
  f_gx_form_top_tvd: {
    ts_type: "number",
  },
  f_gx_form_x_coordinate: {
    ts_type: "number",
  },
  f_gx_form_y_coordinate: {
    ts_type: "number",
  },
  f_gx_gross_thickness: {
    ts_type: "number",
  },
  f_gx_internal_no: {
    ts_type: "number",
  },
  f_gx_net_thickness: {
    ts_type: "number",
  },
  f_gx_porosity: {
    ts_type: "number",
  },
  f_gx_show: {
    ts_type: "string",
  },
  f_gx_strat_column: {
    ts_type: "string",
  },
  f_gx_top_subsea: {
    ts_type: "number",
  },
  f_gx_true_strat_thickness: {
    ts_type: "number",
  },
  f_gx_true_vert_thickness: {
    ts_type: "number",
  },
  f_gx_user1: {
    ts_type: "string",
  },
  f_gx_user2: {
    ts_type: "string",
  },
  f_gx_user3: {
    ts_type: "string",
  },
  f_gx_user4: {
    ts_type: "number",
  },
  f_gx_user5: {
    ts_type: "number",
  },
  f_gx_user6: {
    ts_type: "number",
  },
  f_gx_vendor_no: {
    ts_type: "number",
  },
  f_gx_wellbore_angle: {
    ts_type: "number",
  },
  f_gx_wellbore_azimuth: {
    ts_type: "number",
  },
  f_percent_thickness: {
    ts_type: "number",
  },
  f_pick_location: {
    ts_type: "string",
  },
  f_pick_qualifier: {
    ts_type: "string",
  },
  f_pick_quality: {
    ts_type: "string",
  },
  f_pick_type: {
    ts_type: "string",
  },
  f_public: {
    ts_type: "string",
  },
  f_remark: {
    ts_type: "string",
  },
  f_row_changed_date: {
    ts_type: "date",
  },
  f_source: {
    ts_type: "string",
  },
  f_unc_fault_obs_no: {
    ts_type: "number",
  },
  f_unc_fault_source: {
    ts_type: "string",
  },
  f_unconformity_name: {
    ts_type: "string",
  },
  f_uwi: {
    ts_type: "string",
  },

  // FORMATION

  m_public: {
    ts_type: "string",
  },
  m_unconformity_name: {
    ts_type: "string",
  },
};

const prefixes = {
  w_: "well",
  f_: "well_formation",
  m_: "formation",
};

const global_id_keys = ["w_uwi", "f_form_id", "f_form_obs_no", "f_source"];

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
