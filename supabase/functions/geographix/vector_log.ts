import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter, recency) => {
  filter = filter ? filter : "";
  const whereClause = ["WHERE 1=1"];
  if (filter.trim().length > 0) {
    whereClause.push(filter);
  }
  const where = whereClause.join(" AND ");

  let whereRecent = "";
  if (recency > 0) {
    whereRecent = `WHERE date_modified >= DATEADD(DAY, -${recency}, CURRENT DATE)`;
  }

  let select = `SELECT * FROM (
    WITH 
    w AS (
      SELECT
        uwi               AS w_uwi
      FROM well
    ),
    c AS (
      SELECT 
        wellid            AS c_wellid,
        curveset          AS c_curveset,
        curvename         AS c_curvename,
        version           AS c_version,
        cmd_type          AS c_cmd_type,
        curve_uom         AS c_curve_uom,
        curve_ouom        AS c_curve_ouom,
        date_modified     AS c_date_modified,
        description       AS c_description,
        tool_type         AS c_tool_type,
        remark            AS c_remark,
        topdepth          AS c_topdepth,
        basedepth         AS c_basedepth
      FROM gx_well_curve
      ${whereRecent}
    ),
    s AS (
      SELECT
        wellid            AS s_wellid,
        curveset          AS s_curveset,
        topdepth          AS s_topdepth,
        basedepth         AS s_basedepth,
        depthincr         AS s_depthincr,
        log_job           AS s_log_job,
        log_trip          AS s_log_trip,
        source_file       AS s_source_file,
        remark            AS s_remark,
        type              AS s_type,
        fielddata         AS s_fielddata,
        [import date]     AS s_import_date
      FROM gx_well_curveset
    ),
    v AS (
      SELECT
        wellid            AS v_wellid,
        curveset          AS v_curveset,
        curvename         AS v_curvename,
        version           AS v_version,
        curve_values      AS v_curve_values,
        curve_values      AS v_curve_values_orig
      FROM gx_well_curve_values
    )
    SELECT
      w.*,
      c.*,
      s.*,
      v.*
    FROM w
    JOIN c ON
      w.w_uwi = c.c_wellid
    JOIN v ON
      c.c_wellid = v.v_wellid AND 
      c.c_curveset = v.v_curveset AND 
      c.c_curvename = v.v_curvename AND 
      c.c_version = v.v_version
    JOIN s ON
      c.c_wellid = s.s_wellid AND 
      c.c_curveset = s.s_curveset
    ) x`;

  const order = `ORDER BY w_uwi, c_curveset, c_curvename, c_version`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

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
      return val.replace(/[\u0000-\u001F\u007F-\u009F]/g, "");
    } else if (type === "number") {
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

  c_basedepth: {
    ts_type: "number",
  },
  c_cmd_type: {
    ts_type: "string",
  },
  c_curve_ouom: {
    ts_type: "string",
  },
  c_curve_uom: {
    ts_type: "string",
  },
  c_curvename: {
    ts_type: "string",
  },
  c_curveset: {
    ts_type: "string",
  },
  c_date_modified: {
    ts_type: "date",
  },
  c_description: {
    ts_type: "string",
  },
  c_remark: {
    ts_type: "string",
  },
  c_tool_type: {
    ts_type: "string",
  },
  c_topdepth: {
    ts_type: "number",
  },
  c_version: {
    ts_type: "number",
  },
  c_wellid: {
    ts_type: "string",
  },

  // GX_WELL_CURVESET

  s_basedepth: {
    ts_type: "number",
  },
  s_curveset: {
    ts_type: "string",
  },
  s_depthincr: {
    ts_type: "number",
  },
  s_fielddata: {
    ts_type: "number",
  },
  s_import_date: {
    ts_type: "date",
  },
  s_log_job: {
    ts_type: "number",
  },
  s_log_trip: {
    ts_type: "string",
  },
  s_remark: {
    ts_type: "string",
  },
  s_source_file: {
    ts_type: "string",
  },
  s_topdepth: {
    ts_type: "number",
  },
  s_type: {
    ts_type: "string",
  },
  s_wellid: {
    ts_type: "string",
  },

  // GX_WELL_CURVE_VALUES

  v_curve_values: {
    ts_type: "number",
    xform: "decode_curve_values",
  },
  v_curve_values_orig: {
    ts_type: "string",
    xform: "blob_to_hex",
  },
  v_curvename: {
    ts_type: "string",
  },
  v_curveset: {
    ts_type: "string",
  },
  v_version: {
    ts_type: "number",
  },
  v_wellid: {
    ts_type: "string",
  },
};

const prefixes = {
  w_: "well",
  c_: "gx_well_curve",
  s_: "gx_well_curveset",
  v_: "gx_well_curve_values",
};

const asset_id_keys = ["w_uwi", "c_curveset", "c_curvename", "c_version"];

const well_id_keys = ["w_uwi"];

const default_chunk = 100; // 1000

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter, recency) => {
  return {
    asset_id_keys: asset_id_keys,
    default_chunk: default_chunk,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter, recency),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
