import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  const D = "|&|";
  const N = "purrNULL";

  filter = filter ? filter : "";

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT * FROM (
    WITH w AS (
      SELECT
        uwi                        AS w_uwi
      FROM well
    ),
    i AS (
      SELECT
        base_md                    AS i_base_md,
        base_tvd                   AS i_base_tvd,
        base_x                     AS i_base_x,
        base_y                     AS i_base_y,
        md                         AS i_md,
        mdss                       AS i_mdss,
        remark                     AS i_remark,
        row_changed_date           AS i_row_changed_date,
        source                     AS i_source,
        top_md                     AS i_top_md,
        top_tvd                    AS i_top_tvd,
        top_x                      AS i_top_x,
        top_y                      AS i_top_y,
        tvd                        AS i_tvd,
        tvdss                      AS i_tvdss,
        uwi                        AS i_uwi,
        x                          AS i_x,
        y                          AS i_y,
        zone_id                    AS i_zone_id,
        zone_name                  AS i_zone_name
      FROM well_zone_interval
    ),
    v AS (
      SELECT 
        v.uwi                      AS j_uwi,
        v.zone_name                AS j_zone_name, 
        LIST(IFNULL(v.gx_remark,                '${N}', v.gx_remark),                                 '${D}' ORDER BY v.zattribute_name)  AS v_gx_remark,
        LIST(IFNULL(v.row_changed_date,         '${N}', v.row_changed_date),                          '${D}' ORDER BY v.zattribute_name)  AS v_row_changed_date,
        LIST(IFNULL(v.uwi,                      '${N}', v.uwi),                                       '${D}' ORDER BY v.zattribute_name)  AS v_uwi,
        LIST(IFNULL(v.zattribute_name,          '${N}', v.zattribute_name),                           '${D}' ORDER BY v.zattribute_name)  AS v_zattribute_name,
        LIST(IFNULL(v.zattribute_value_date,    '${N}', CAST(v.zattribute_value_date AS VARCHAR)),    '${D}' ORDER BY v.zattribute_name)  AS v_zattribute_value_date,
        LIST(IFNULL(v.zattribute_value_numeric, '${N}', CAST(v.zattribute_value_numeric AS VARCHAR)), '${D}' ORDER BY v.zattribute_name)  AS v_zattribute_value_numeric,
        LIST(IFNULL(v.zattribute_value_string,  '${N}', v.zattribute_value_string),                   '${D}' ORDER BY v.zattribute_name)  AS v_zattribute_value_string,
        LIST(IFNULL(v.zone_name,                '${N}', v.zone_name),                                 '${D}' ORDER BY v.zattribute_name)  AS v_zone_name,
        LIST(IFNULL(a.gx_remark,                '${N}', a.gx_remark),                                 '${D}')                             AS a_gx_remark,
        LIST(IFNULL(a.row_changed_date,         '${N}', CAST(a.row_changed_date AS VARCHAR)),         '${D}')                             AS a_row_changed_date,
        LIST(IFNULL(a.zattribute_name,          '${N}', a.zattribute_name),                           '${D}' ORDER BY v.zattribute_name)  AS a_zattribute_name,
        LIST(IFNULL(a.zone_name,                '${N}', a.zone_name),                                 '${D}' ORDER BY v.zattribute_name)  AS a_zone_name,
        LIST(IFNULL(g.domain,                   '${N}', g.domain),                                    '${D}')                             AS g_domain,
        LIST(IFNULL(g.gx_remark,                '${N}', g.gx_remark),                                 '${D}')                             AS g_gx_remark,
        LIST(IFNULL(g.row_changed_date,         '${N}', CAST(g.row_changed_date AS VARCHAR)),         '${D}')                             AS g_row_changed_date,
        LIST(IFNULL(g.zattribute_decimals,      '${N}', CAST(g.zattribute_decimals AS VARCHAR)),      '${D}')                             AS g_zattribute_decimals,
        LIST(IFNULL(g.zattribute_name,          '${N}', g.zattribute_name),                           '${D}')                             AS g_zattribute_name,
        LIST(IFNULL(g.zattribute_type,          '${N}', CAST(g.zattribute_type AS VARCHAR)),          '${D}')                             AS g_zattribute_type,
        LIST(IFNULL(g.zattribute_value_unit,    '${N}', g.zattribute_value_unit),                     '${D}')                             AS g_zattribute_value_unit
      FROM well_zone_intrvl_value v
      JOIN gx_zone_zattribute a ON v.zone_name = a.zone_name AND v.zattribute_name = a.zattribute_name
      JOIN gx_zattribute g ON a.zattribute_name = g.zattribute_name
      GROUP BY j_uwi, j_zone_name
    ),
    z AS (
      SELECT
        domain                     AS z_domain,
        gx_base_form_name          AS z_gx_base_form_name,
        gx_base_form_tb            AS z_gx_base_form_tb,
        gx_base_modifier           AS z_gx_base_modifier,
        gx_base_offset             AS z_gx_base_offset,
        gx_remark                  AS z_gx_remark,
        gx_top_form_name           AS z_gx_top_form_name,
        gx_top_form_tb             AS z_gx_top_form_tb,
        gx_top_modifier            AS z_gx_top_modifier,
        gx_top_offset              AS z_gx_top_offset,
        gx_topmidbase              AS z_gx_topmidbase,
        row_changed_date           AS z_row_changed_date,
        zone_name                  AS z_zone_name
      FROM gx_zone
    )
  SELECT
    w.*,
    i.*,
    v.*,
    z.*
  FROM  w
  JOIN i ON i.i_uwi = w.w_uwi
  FULL OUTER JOIN v ON v.j_uwi = i.i_uwi  AND i.i_zone_name = v.j_zone_name
  JOIN z ON i.i_zone_name = z.z_zone_name
    ) x`;

  const order = `ORDER BY w_uwi, i_zone_name`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  return {
    select: select,
    count: count,
    order: order,
    where: where,
  };
};

const xformer = (args) => {
  const D = "|&|";
  const N = "purrNULL";

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
    ts_type: "string",
  },

  // WELL_ZONE_INTERVAL

  i_base_md: {
    ts_type: "number",
  },
  i_base_tvd: {
    ts_type: "number",
  },
  i_base_x: {
    ts_type: "number",
  },
  i_base_y: {
    ts_type: "number",
  },
  i_md: {
    ts_type: "number",
  },
  i_mdss: {
    ts_type: "number",
  },
  i_remark: {
    ts_type: "string",
  },
  i_row_changed_date: {
    ts_type: "date",
  },
  i_source: {
    ts_type: "string",
  },
  i_top_md: {
    ts_type: "number",
  },
  i_top_tvd: {
    ts_type: "number",
  },
  i_top_x: {
    ts_type: "number",
  },
  i_top_y: {
    ts_type: "number",
  },
  i_tvd: {
    ts_type: "number",
  },
  i_tvdss: {
    ts_type: "number",
  },
  i_uwi: {
    ts_type: "string",
  },
  i_x: {
    ts_type: "number",
  },
  i_y: {
    ts_type: "number",
  },
  i_zone_id: {
    ts_type: "string",
  },
  i_zone_name: {
    ts_type: "string",
  },

  // WELL_ZONE_INTRVL_VALUE

  j_uwi: {
    ts_type: "string",
  },
  j_zone_name: {
    ts_type: "string",
  },
  v_gx_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  v_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  v_zattribute_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  v_zattribute_value_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  v_zattribute_value_numeric: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  v_zattribute_value_string: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  v_uwi: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  v_zone_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },

  // GX_ZATTRIBUTE

  g_domain: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  g_gx_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  g_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  g_zattribute_decimals: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  g_zattribute_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  g_zattribute_type: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  g_zattribute_value_unit: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },

  // GX_ZONE

  z_domain: {
    ts_type: "string",
  },
  z_gx_base_form_name: {
    ts_type: "string",
  },
  z_gx_base_form_tb: {
    ts_type: "number",
  },
  z_gx_base_modifier: {
    ts_type: "string",
  },
  z_gx_base_offset: {
    ts_type: "number",
  },
  z_gx_remark: {
    ts_type: "string",
  },
  z_gx_top_form_name: {
    ts_type: "string",
  },
  z_gx_top_form_tb: {
    ts_type: "number",
  },
  z_gx_top_modifier: {
    ts_type: "string",
  },
  z_gx_top_offset: {
    ts_type: "number",
  },
  z_gx_topmidbase: {
    ts_type: "number",
  },
  z_row_changed_date: {
    ts_type: "date",
  },
  z_zone_name: {
    ts_type: "string",
  },

  // GX_ZONE_ZATTRIBUTE

  a_gx_remark: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_row_changed_date: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  a_zattribute_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  a_zone_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
};

const prefixes = {
  w_: "well",
  i_: "well_zone_interval",
  v_: "well_zone_intrvl_value",
  g_: "gx_zattribute",
  z_: "gx_zone",
  a_: "gx_zone_zattribute",
};

const asset_id_keys = ["w_uwi", "i_zone_name"];

const well_id_keys = ["w_uwi"];

const default_chunk = 500;

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    asset_id_keys: asset_id_keys,
    default_chunk: default_chunk,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
