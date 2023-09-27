import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const D = "|&|";
  const N = "purrNULL";
  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn"];
  const idForm = idCols
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT
    w.wsn          AS w_wsn,
    w.uwi          AS w_uwi,
    LIST(COALESCE(CAST(c.recid AS VARCHAR(10)),    '${N}'), '${D}') AS c_recid,
    LIST(COALESCE(CAST(c.wsn AS VARCHAR(10)),      '${N}'), '${D}') AS c_wsn,
    LIST(COALESCE(CAST(c.flags AS VARCHAR(10)),    '${N}'), '${D}') AS c_flags,
    LIST(COALESCE(CAST(c.lithcode AS VARCHAR(10)), '${N}'), '${D}') AS c_lithcode,
    LIST(COALESCE(CAST(c.date AS VARCHAR(10)),     '${N}'), '${D}') AS c_date,
    LIST(COALESCE(CAST(c.top AS VARCHAR(10)),      '${N}'), '${D}') AS c_top,
    LIST(COALESCE(CAST(c.base AS VARCHAR(10)),     '${N}'), '${D}') AS c_base,
    LIST(COALESCE(CAST(c.recover AS VARCHAR(10)),  '${N}'), '${D}') AS c_recover,
    LIST(COALESCE(c.type,                          '${N}'), '${D}') AS c_type,
    LIST(COALESCE(c.qual,                          '${N}'), '${D}') AS c_qual,
    LIST(COALESCE(c.fmname,                        '${N}'), '${D}') AS c_fmname,
    LIST(COALESCE(c.desc,                          '${N}'), '${D}') AS c_desc,
    LIST(COALESCE(CAST(c.remark AS VARCHAR(512)),  '${N}'), '${D}') AS c_remark
  FROM well w
  JOIN cores c ON c.wsn = w.wsn
  ${where_clause_stub}
  GROUP BY w.wsn
  `;

  const order = `ORDER BY w_uwi`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  //const fast_count = `SELECT COUNT(DISTINCT uwi) AS count FROM well`;

  // NOTE: key vs keylist (purrio_client batcher figures it out)
  const identifier = `
    SELECT
      DISTINCT(c.wsn) AS key
    FROM well w
    JOIN cores c ON w.wsn = c.wsn
    ${where}`;

  return {
    identifier: identifier,
    id_cols: idCols,
    where_clause_stub: where_clause_stub,
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
    case "delimited_array_of_excel_dates":
      return (() => {
        const localX = (v) => {
          if (v.toString().match(/1[eE]\+?30/i)) {
            return null;
          }
          const d = new Date(Math.round((v - 25569) * 86400 * 1000));
          return d.toISOString();
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
    case "memo_to_string":
      return (() => {
        try {
          const buf = Buffer.from(obj[key], "binary");
          return ensureType("string", buf.toString("utf-8"));
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

  // CORES

  c_recid: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_wsn: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_flags: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_lithcode: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_date: {
    ts_type: "date",
    xform: "delimited_array_of_excel_dates",
  },
  c_top: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_base: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_recover: {
    ts_type: "number",
    xform: "delimited_array_with_nulls",
  },
  c_type: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_qual: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_fmname: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_desc: {
    ts_type: "string",
    xform: "delimited_array_with_nulls",
  },
  c_remark: {
    ts_type: "string",
    xform: "delimited_array_of_memo",
  },
};

const prefixes = {
  w_: "well",
  c_: "cores",
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
