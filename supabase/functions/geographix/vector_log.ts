import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT * FROM (
    WITH 
    w AS (
      SELECT
        uwi               AS w_uwi,
        assigned_field    AS w_assigned_field,
        common_well_name  AS w_common_well_name,
        completion_date   AS w_completion_date,
        country           AS w_country,
        county            AS w_county,
        current_class     AS w_current_class,
        current_status    AS w_current_status,
        depth_datum       AS w_depth_datum,
        final_td          AS w_final_td,
        ground_elev       AS w_ground_elev,
        kb_elev           AS w_kb_elev,
        lease_name        AS w_lease_name,
        operator          AS w_operator,
        province_state    AS w_province_state,
        row_changed_date  AS w_row_changed_date,
        spud_date         AS w_spud_date,
        surface_latitude  AS w_surface_latitude,
        surface_longitude AS w_surface_longitude,
        td_form           AS w_td_form,
        well_name         AS w_well_name,
        well_number       AS w_well_number        
      FROM well
    ),
    wc AS (
      SELECT 
        wellid            AS wc_wellid,
        curveset          AS wc_curveset,
        curvename         AS wc_curvename,
        version           AS wc_version,
        cmd_type          AS wc_cmd_type,
        curve_uom         AS wc_curve_uom,
        curve_ouom        AS wc_curve_ouom,
        date_modified     AS wc_date_modified,
        description       AS wc_description,
        tool_type         AS wc_tool_type,
        remark            AS wc_remark,
        topdepth          AS wc_topdepth,
        basedepth         AS wc_basedepth
      FROM gx_well_curve
    ),
    wcs AS (
      SELECT
        wellid            AS wcs_wellid,
        curveset          AS wcs_curveset,
        topdepth          AS wcs_topdepth,
        basedepth         AS wcs_basedepth,
        depthincr         AS wcs_depthincr,
        log_job           AS wcs_log_job,
        log_trip          AS wcs_log_trip,
        source_file       AS wcs_source_file,
        remark            AS wcs_remark,
        type              AS wcs_type,
        fielddata         AS wcs_fielddata,
        [import date]     AS wcs_import_date
      FROM gx_well_curveset
    ),
    wcv AS (
      SELECT
        wellid            AS wcv_wellid,
        curveset          AS wcv_curveset,
        curvename         AS wcv_curvename,
        version           AS wcv_version,
        curve_values      AS wcv_curve_values,
        curve_values      AS wcv_curve_values_orig
      FROM gx_well_curve_values
    )
    SELECT
      w.*,
      wc.*,
      wcs.*,
      wcv.*
    FROM w
    JOIN wc ON
      w.w_uwi = wc.wc_wellid
    JOIN wcv ON
      wc.wc_wellid = wcv.wcv_wellid AND 
      wc.wc_curveset = wcv.wcv_curveset AND 
      wc.wc_curvename = wcv.wcv_curvename AND 
      wc.wc_version = wcv.wcv_version
    JOIN wcs ON
      wc.wc_wellid = wcs.wcs_wellid AND 
      wc.wc_curveset = wcs.wcs_curveset
    ) x`;

  const order = `ORDER BY w_uwi, wc_curveset, wc_curvename, wc_version`;

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
  let { func, key, arg, obj } = args;

  if (obj[key] == null) {
    return null;
  }

  switch (func) {
    case "blob_to_string":
      return (() => {
        try {
          return Buffer.from(obj[key]).toString("hex");
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    case "decode_curve_values":
      return (() => {
        try {
          const vals = [];
          const buf = Buffer.from(obj[key], "binary");
          for (let i = 2; i < buf.length; i += 4) {
            let val = buf.slice(i, i + 4).readFloatLE();
            vals.push(val);
          }
          return vals;
        } catch (error) {
          console.log(error);
          return;
        }
      })();
    default:
      console.warn("UNEXPECTED TRANSFORM FUNC: " + func);
      return obj[key];
  }
};

const xforms = {
  wcv_curve_values: "decode_curve_values",
  wcv_curve_values_orig: "blob_to_string",
};

const prefixes = {
  w_: "well",
  wc_: "gx_well_curve",
  wcs_: "gx_well_curveset",
  wcv_: "gx_well_curve_values",
};

const global_id_keys = ["w_uwi", "wc_curveset", "wc_curvename", "wc_version"];

const well_id_keys = ["w_uwi"];

const pg_cols = ["id", "repo_id", "well_id", "geo_type", "tag", "doc"];

const default_chunk = 5000;

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
