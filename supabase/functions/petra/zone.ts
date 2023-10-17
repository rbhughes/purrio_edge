import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

// yep, it's very slow

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const D = "|&|";
  const N = "purrNULL";
  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn", "n.zid"];
  const idForm = idCols
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT
    w.wsn          AS w_wsn,
    w.uwi          AS w_uwi,
  
    n.zid          AS n_zid,
    n.name         AS n_name,
    n.desc         AS n_desc,
    n.kind         AS n_kind,
    n.umode        AS n_umode,
    n.lmode        AS n_lmode,
    n.utopid       AS n_utopid,
    n.ltopid       AS n_ltopid,
    n.udepth       AS n_udepth,
    n.ldepth       AS n_ldepth,
    n.uoffset      AS n_uoffset,
    n.loffset      AS n_loffset,
    n.adddate      AS n_adddate,
    n.chgdate      AS n_chgdate,
    n.remarks      AS n_remarks,
  
    LIST(COALESCE(CAST(f.fid AS VARCHAR(10)),       '${N}'), '${D}') AS f_fid,
    LIST(COALESCE(CAST(f.zid AS VARCHAR(10)),       '${N}'), '${D}') AS f_zid,
    LIST(COALESCE(f.name,                           '${N}'), '${D}') AS f_name,
    LIST(COALESCE(f.source,                         '${N}'), '${D}') AS f_source,
    LIST(COALESCE(f.desc,                           '${N}'), '${D}') AS f_desc,
    LIST(COALESCE(f.units,                          '${N}'), '${D}') AS f_units,
    LIST(COALESCE(f.kind,                           '${N}'), '${D}') AS f_kind,
    LIST(COALESCE(CAST(f.ndec AS VARCHAR(20)),      '${N}'), '${D}') AS f_ndec,
    LIST(COALESCE(CAST(f.adddate AS VARCHAR(20)),   '${N}'), '${D}') AS f_adddate,
    LIST(COALESCE(CAST(f.chgdate AS VARCHAR(20)),   '${N}'), '${D}') AS f_chgdate,
    LIST(COALESCE(f.remarks,                        '${N}'), '${D}') AS f_remarks,
    LIST(COALESCE(CAST(f.flags AS VARCHAR(20)),     '${N}'), '${D}') AS f_flags,
    LIST(COALESCE(CAST(f.unitstype AS VARCHAR(20)), '${N}'), '${D}') AS f_unitstype,
    
    LIST(COALESCE(CAST(z.fid AS VARCHAR(20)),       '${N}'), '${D}') AS z_fid,
    LIST(COALESCE(CAST(z.wsn AS VARCHAR(20)),       '${N}'), '${D}') AS z_wsn,
    LIST(COALESCE(CAST(z.zid AS VARCHAR(20)),       '${N}'), '${D}') AS z_zid,
    LIST(COALESCE(CAST(z.z AS VARCHAR(20)),         '${N}'), '${D}') AS z_z,
    LIST(COALESCE(CAST(z.postdepth AS VARCHAR(20)), '${N}'), '${D}') AS z_postdepth,
    LIST(COALESCE(z.quality,                        '${N}'), '${D}') AS z_quality,
    LIST(COALESCE(CAST(z.symbol AS VARCHAR(20)),    '${N}'), '${D}') AS z_symbol,
    LIST(COALESCE(CAST(z.chgdate AS VARCHAR(20)),   '${N}'), '${D}') AS z_chgdate,
    LIST(COALESCE(CAST(z.textlen AS VARCHAR(20)),   '${N}'), '${D}') AS z_textlen,
    LIST(COALESCE(z.text,                           '${N}'), '${D}') AS z_text,
    LIST(COALESCE(CAST(z.datalen AS VARCHAR(20)),   '${N}'), '${D}') AS z_datalen,
    LIST(COALESCE(CAST(z.data AS VARCHAR(512)),     '${N}'), '${D}') AS z_data

  FROM well w
  JOIN zdata z ON z.wsn = w.wsn
  JOIN zonedef n ON n.zid = z.zid AND n.kind > 2
  JOIN zflddef f ON f.zid = n.zid AND f.fid = z.fid

  ${where_clause_stub}
  GROUP BY w.wsn, n.zid
  `;

  const order = `ORDER BY w_uwi, n_zid`;

  // NOTE: key, not keylist
  const identifier = `
    SELECT
      ${idForm} AS key
    FROM well w
    JOIN zdata z ON z.wsn = w.wsn
    JOIN zonedef n ON n.zid = z.zid AND n.kind > 2
    JOIN zflddef f ON f.zid = n.zid AND f.fid = z.fid
    ${where}
    GROUP BY key
    `;

  return {
    id_cols: idCols,
    identifier: identifier,
    order: order,
    select: select,
    where: where,
    where_clause_stub: where_clause_stub,
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
    case "delimited_array_of_memo":
      return (() => {
        const localX = (v) => {
          const buf = Buffer.from(v, "binary");
          return ensureType("string", buf.toString("utf-8"));
        };
        try {
          return obj[key].split(D).map((v) => (v === N ? null : localX(v)));
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    case "delimited_array_of_hex":
      return (() => {
        const localX = (v) => {
          return Buffer.from(v).toString("hex");
        };
        try {
          return obj[key].split(D).map((v) => (v === N ? null : localX(v)));
        } catch (error) {
          console.log("ERROR", error);
          return;
        }
      })();
    case "excel_date":
      return (() => {
        try {
          if (obj[key].toString().match(/1[eE]\+?30/i)) {
            return null;
          }
          const d = new Date(Math.round((obj[key] - 25569) * 86400 * 1000));
          return d.toISOString();
        } catch (error) {
          console.log("ERROR", error);
          return null;
        }
      })();
    default:
      return ensureType(typ, obj[key]);
  }
};

const xforms = {
  // WELL

  w_wsn: {
    ts_type: "number",
  },
  w_uwi: {
    ts_type: "string",
  },

  // ZDATA

  z_fid: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  z_wsn: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  z_zid: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  z_z: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  z_postdepth: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  z_quality: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  z_symbol: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  z_chgdate: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  z_textlen: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  z_text: {
    ts_type: "string",
    xform: "delimited_array_of_memo",
  },
  z_datalen: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  z_data: {
    ts_type: "string",
    xform: "delimited_array_of_hex",
  },

  // ZONEDEF

  n_zid: {
    ts_type: "number",
  },
  n_name: {
    ts_type: "string",
  },
  n_desc: {
    ts_type: "string",
  },
  n_kind: {
    ts_type: "number",
  },
  n_umode: {
    ts_type: "string",
  },
  n_lmode: {
    ts_type: "string",
  },
  n_utopid: {
    ts_type: "number",
  },
  n_ltopid: {
    ts_type: "number",
  },
  n_udepth: {
    ts_type: "number",
  },
  n_ldepth: {
    ts_type: "number",
  },
  n_uoffset: {
    ts_type: "number",
  },
  n_loffset: {
    ts_type: "number",
  },
  n_adddate: {
    ts_type: "date",
    xform: "excel_date",
  },
  n_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  n_remarks: {
    ts_type: "string",
    xform: "memo_to_string",
  },

  // ZFLDDEF

  f_fid: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_zid: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_name: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_source: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_units: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_kind: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  f_ndec: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_adddate: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  f_chgdate: {
    ts_type: "date",
    xform: "delimited_array_with_nulls",
  },
  f_remarks: {
    ts_type: "string",
    xform: "delimited_array_of_memo",
  },
  f_flags: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  f_unitstype: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
};

const prefixes = {
  w_: "well",
  n_: "zonedef",
  f_: "zflddef",
  z_: "zdata",
};

const asset_id_keys = ["w_uwi", "n_zid"];

const well_id_keys = ["w_uwi"];

const default_chunk = 100; // 200

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
