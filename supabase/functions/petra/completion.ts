import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn, p.recid"];
  const idForm = idCols
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT
    w.wsn          AS w_wsn,
    w.uwi          AS w_uwi,
    p.recid        AS p_recid,
    p.wsn          AS p_wsn,
    p.flags        AS p_flags,
    p.date         AS p_date,
    p.enddate      AS p_enddate,
    p.top          AS p_top,
    p.base         AS p_base,
    p.diameter     AS p_diameter,
    p.numshots     AS p_numshots,
    p.method       AS p_method,
    p.comptype     AS p_comptype,
    p.perftype     AS p_perftype,
    p.remark       AS p_remark,
    p.fmname       AS p_fmname,
    p.chgdate      AS p_chgdate,
    p.source       AS p_source
  FROM well w
  JOIN perfs p ON p.wsn = w.wsn
  ${where_clause_stub}
  `;

  const order = `ORDER BY w_uwi`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  //const fast_count = `SELECT COUNT(DISTINCT uwi) AS count FROM well`;

  const identifier = `
    SELECT
      LIST(${idForm}) AS keylist
    FROM well w
    JOIN perfs p ON p.wsn = w.wsn
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

// There are multiple IP tests per well. We would normally roll them up using
// LIST aggregation, but each test may contain multiple well treatments
// (PDTEST.treat), which are stored as BLOBs, one per test. LIST cannot handle
// BLOBs. Instead, we collect all tests and then aggregate them from the docs.
// TODO: figure out a way to avoid second docs pass?
const reduceByField = (docs) => {
  const parentName = "well";
  const arrayName = "perfs";
  const linkName = "wsn";

  try {
    let keep = new Map();
    for (const o of docs) {
      keep.set(o[parentName][linkName], {
        [arrayName]: [],
      });
      for (const [n, v] of Object.entries(o)) {
        if (n !== arrayName) {
          keep.get(o[parentName][linkName])[n] = v;
        }
      }
    }
    for (const o of docs) {
      for (const [n, v] of Object.entries(o)) {
        if (n === arrayName) {
          keep.get(o[parentName][linkName])[n].push(v);
        }
      }
    }

    return Array.from(keep.values());
  } catch (error) {
    console.log("ERROR", error);
    return null;
  }
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
    case "excel_date":
      return (() => {
        try {
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

  // PDTEST

  p_recid: {
    ts_type: "number",
  },
  p_wsn: {
    ts_type: "number",
  },
  p_flags: {
    ts_type: "number",
  },
  p_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  p_enddate: {
    ts_type: "date",
    xform: "excel_date",
  },
  p_top: {
    ts_type: "number",
  },
  p_base: {
    ts_type: "number",
  },
  p_diameter: {
    ts_type: "number",
  },
  p_numshots: {
    ts_type: "number",
  },
  p_method: {
    ts_type: "string",
  },
  p_comptype: {
    ts_type: "string",
  },
  p_perftype: {
    ts_type: "string",
  },
  p_remark: {
    xform: "memo_to_string",
  },
  p_fmname: {
    ts_type: "string",
  },
  p_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  p_source: {
    ts_type: "string",
  },
};

const prefixes = {
  w_: "well",
  p_: "pdtest",
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
    serialized_doc_processor: serialize(reduceByField),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
