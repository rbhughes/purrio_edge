import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn", "a.ldsn"];
  const idForm = idCols
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT
    w.wsn          AS w_wsn,
    w.uwi          AS w_uwi,
    a.ldsn         AS a_ldsn,
    a.wsn          AS a_wsn,
    a.lsn          AS a_lsn,
    a.flags        AS a_flags,
    a.units        AS a_units,
    a.elev_zid     AS a_elev_zid,
    a.elev_fid     AS a_elev_fid,
    a.numpts       AS a_numpts,
    a.start        AS a_start,
    a.stop         AS a_stop,
    a.step         AS a_step,
    a.minval       AS a_minval,
    a.maxval       AS a_maxval,
    a.mean         AS a_mean,
    a.stddev       AS a_stddev,
    a.nullval      AS a_nullval,
    a.source       AS a_source,
    a.digits       AS a_digits,
    a.remarks      AS a_remarks,
    f.lsn          AS f_lsn,
    f.logname      AS f_logname,
    f.desc         AS f_desc,
    f.units        AS f_units,
    f.servid       AS f_servid,
    f.remarks      AS f_remarks,
    f.flags        AS f_flags,
    x.ldsn         AS x_ldsn,
    x.wsn          AS x_wsn,
    x.lsn          AS x_lsn,
    x.flags        AS x_flags,
    x.adddate      AS x_adddate,
    x.chgdate      AS x_chgdate,
    x.lasid        AS x_lasid,
    s.lasid        AS s_lasid,
    s.wsn          AS s_wsn,
    s.flags        AS s_flags,
    s.adddate      AS s_adddate,
    s.chgdate      AS s_chgdate,
    s.hdrsize      AS s_hdrsize,
    s.lashdr       AS s_lashdr
  FROM well w
  JOIN logdata a ON w.wsn = a.wsn
  JOIN logdef f ON a.lsn = f.lsn
  JOIN logdatax x ON a.wsn = x.wsn AND a.lsn = x.lsn AND a.ldsn = x.ldsn
  LEFT OUTER JOIN loglas s ON x.lasid = s.lasid AND w.wsn = s.wsn
  ${where_clause_stub}
  `;

  const order = `ORDER BY w_uwi, a_ldsn`;

  const count = `SELECT COUNT(*) AS count FROM ( ${select} ) c ${where}`;

  //const fast_count = `SELECT COUNT(DISTINCT uwi) AS count FROM well`;

  const identifier = `
    SELECT
      LIST(${idForm}) as keylist
    FROM well w
    JOIN logdata a ON w.wsn = a.wsn
    JOIN logdef f ON a.lsn = f.lsn
    JOIN logdatax x ON a.wsn = x.wsn AND a.lsn = x.lsn AND a.ldsn = x.ldsn
    LEFT OUTER JOIN loglas s ON x.lasid = s.lasid AND w.wsn = s.wsn
    ${where}
    `;

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

  if (obj[key] == null) {
    return null;
  }

  switch (func) {
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
    case "logdata_digits":
      return (() => {
        try {
          const digits = [];
          const b = Buffer.from(obj[key]);
          for (let i = 0; i < b.length; i += 8) {
            let val = b.subarray(i, i + 8).readDoubleLE();
            digits.push(val);
          }
          return digits;
        } catch (error) {
          console.error("ERROR", error);
          return null;
        }
      })();
    case "loglas_lashdr":
      return (() => {
        try {
          const a = Buffer.from(obj[key]).toString("utf-8").split(";");
          const b = a.map((r) => r.replace(/^\"/, "").replace(/\"$/, ""));
          return ensureType("string", b.join("\n"));
        } catch (error) {
          console.error("ERROR", error);
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

  // LOGDATA

  a_ldsn: {
    ts_type: "number",
  },
  a_wsn: {
    ts_type: "number",
  },
  a_lsn: {
    ts_type: "number",
  },
  a_flags: {
    ts_type: "number",
  },
  a_flags: {
    ts_type: "number",
  },
  a_units: {
    ts_type: "number",
  },
  a_elev_zid: {
    ts_type: "number",
  },
  a_elev_fid: {
    ts_type: "number",
  },
  a_numpts: {
    ts_type: "number",
  },
  a_start: {
    ts_type: "number",
  },
  a_stop: {
    ts_type: "number",
  },
  a_step: {
    ts_type: "number",
  },
  a_minval: {
    ts_type: "number",
  },
  a_maxval: {
    ts_type: "number",
  },
  a_mean: {
    ts_type: "number",
  },
  a_stddev: {
    ts_type: "number",
  },
  a_nullval: {
    ts_type: "number",
  },
  a_source: {
    ts_type: "string",
  },
  a_digits: {
    ts_type: "string",
    xform: "logdata_digits",
  },
  a_remarks: {
    ts_type: "string",
    xform: "memo_to_string",
  },

  // LOGDEF

  f_lsn: {
    ts_type: "number",
  },
  f_logname: {
    ts_type: "string",
  },
  f_desc: {
    ts_type: "string",
  },
  f_units: {
    ts_type: "string",
  },
  f_servid: {
    ts_type: "number",
  },
  f_remarks: {
    ts_type: "string",
  },
  f_flags: {
    ts_type: "number",
  },

  // LOGDATAX

  x_ldsn: {
    ts_type: "number",
  },
  x_wsn: {
    ts_type: "number",
  },
  x_lsn: {
    ts_type: "number",
  },
  x_flags: {
    ts_type: "number",
  },
  x_adddate: {
    ts_type: "date",
    xform: "excel_date",
  },
  x_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  x_lasid: {
    ts_type: "number",
  },

  // LOGLAS

  s_lasid: {
    ts_type: "number",
  },
  s_wsn: {
    ts_type: "number",
  },
  s_flags: {
    ts_type: "number",
  },
  s_adddate: {
    ts_type: "date",
    xform: "excel_date",
  },
  s_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  s_hdrsize: {
    ts_type: "number",
  },
  s_lashdr: {
    ts_type: "string",
    xform: "loglas_lashdr",
  },
};

const prefixes = {
  w_: "well",
  a_: "logdata",
  f_: "logdef",
  x_: "logdatax",
  s_: "loglas",
};

const global_id_keys = ["w_uwi", "a_ldsn"];

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
