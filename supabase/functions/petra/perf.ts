import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

// AKA PERFS

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn", "p.recid"];
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

  const identifier = `
    SELECT
      LIST(${idForm}) AS keylist
    FROM well w
    JOIN perfs p ON p.wsn = w.wsn
    ${where}`;

  return {
    id_cols: idCols,
    identifier: identifier,
    order: order,
    select: select,
    where: where,
    where_clause_stub: where_clause_stub,
  };
};

// There are multiple IP tests per well. We would normally roll them up using
// LIST aggregation, but each test may contain multiple well treatments
// (PERFS.treat), which are stored as BLOBs, one per test. LIST cannot handle
// BLOBs. Instead, we collect all tests and then aggregate them from the docs.
const aggregatePERFS = (docs: Record<string, any>[]) => {
  const outputDocs: Record<string, any>[] = [];
  docs.forEach((inputDoc) => {
    const existingDoc = outputDocs.find(
      (outputDoc) => outputDoc.doc.well.wsn === inputDoc.doc.well.wsn
    );
    if (existingDoc) {
      existingDoc.doc.perfs.push(inputDoc.doc.perfs);
    } else {
      outputDocs.push({
        id: inputDoc.id,
        well_id: inputDoc.well_id,
        repo_id: inputDoc.repo_id,
        geo_type: inputDoc.geo_type,
        tag: inputDoc.tag,
        doc: {
          perfs: [inputDoc.doc.perfs],
          well: inputDoc.doc.well,
        },
      });
    }
  });
  return outputDocs;
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

  // PERFS

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
  p_: "perfs",
};

const asset_id_keys = ["w_uwi", "p_recid"];

const well_id_keys = ["w_uwi"];

const default_chunk = 500;

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    asset_id_keys: asset_id_keys,
    default_chunk: default_chunk,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    serialized_doc_processor: serialize(aggregatePERFS),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
