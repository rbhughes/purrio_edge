import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT * FROM (
    WITH 
    w AS (
      SELECT
        uwi               AS w_uwi
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
      return ensureType(typ, obj[key]);
  }
};

const xforms = {
  // WELL

  w_uwi: {
    ts_type: "string",
  },

  // GX_WELL_CURVE

  wc_basedepth: {
    ts_type: "number",
  },
  wc_cmd_type: {
    ts_type: "string",
  },
  wc_curve_ouom: {
    ts_type: "string",
  },
  wc_curve_uom: {
    ts_type: "string",
  },
  wc_curvename: {
    ts_type: "string",
  },
  wc_curveset: {
    ts_type: "string",
  },
  wc_date_modified: {
    ts_type: "date",
  },
  wc_description: {
    ts_type: "string",
  },
  wc_remark: {
    ts_type: "string",
  },
  wc_tool_type: {
    ts_type: "string",
  },
  wc_topdepth: {
    ts_type: "number",
  },
  wc_version: {
    ts_type: "number",
  },
  wc_wellid: {
    ts_type: "string",
  },

  // GX_WELL_CURVESET

  wcs_basedepth: {
    ts_type: "number",
  },
  wcs_curveset: {
    ts_type: "string",
  },
  wcs_depthincr: {
    ts_type: "number",
  },
  wcs_fielddata: {
    ts_type: "number",
  },
  wcs_import_date: {
    ts_type: "date",
  },
  wcs_log_job: {
    ts_type: "number",
  },
  wcs_log_trip: {
    ts_type: "string",
  },
  wcs_remark: {
    ts_type: "string",
  },
  wcs_source_file: {
    ts_type: "string",
  },
  wcs_topdepth: {
    ts_type: "number",
  },
  wcs_type: {
    ts_type: "string",
  },
  wcs_wellid: {
    ts_type: "string",
  },

  // GX_WELL_CURVE_VALUES

  wcv_curve_values: {
    ts_type: "number",
    xform: "decode_curve_values",
  },
  wcv_curve_values_orig: {
    ts_type: "string",
    xform: "blob_to_hex",
  },
  wcv_curvename: {
    ts_type: "string",
  },
  wcv_curveset: {
    ts_type: "string",
  },
  wcv_version: {
    ts_type: "number",
  },
  wcv_wellid: {
    ts_type: "string",
  },
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
