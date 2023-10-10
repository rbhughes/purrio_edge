import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

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
    p.numtreat     AS p_numtreat,
    p.flags        AS p_flags,
    p.date         AS p_date,
    p.top          AS p_top,
    p.base         AS p_base,
    p.oilvol       AS p_oilvol,
    p.gasvol       AS p_gasvol,
    p.wtrvol       AS p_wtrvol,
    p.ftp          AS p_ftp,
    p.fcp          AS p_fcp,
    p.stp          AS p_stp,
    p.scp          AS p_scp,
    p.bht          AS p_bht,
    p.bhp          AS p_bhp,
    p.choke        AS p_choke,
    p.duration     AS p_duration,
    p.caof         AS p_caof,
    p.oilgty       AS p_oilgty,
    p.gasgty       AS p_gasgty,
    p.gor          AS p_gor,
    p.testtype     AS p_testtype,
    p.fmname       AS p_fmname,
    p.oilunit      AS p_oilunit,
    p.gasunit      AS p_gasunit,
    p.wtrunit      AS p_wtrunit,
    p.remark       AS p_remark,
    p.treat        AS p_treat,
    p.chgdate      AS p_chgdate,
    p.unitstype    AS p_unitstype

  FROM well w
  JOIN pdtest p ON p.wsn = w.wsn
  ${where_clause_stub}
  `;

  const order = `ORDER BY w_uwi`;

  const identifier = `
    SELECT
      LIST(${idForm}) AS keylist
    FROM well w
    JOIN pdtest p ON p.wsn = w.wsn
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

// There are multiple DST tests per well. We would normally roll them up using
// LIST aggregation, but each test may contain multiple well treatments
// (FMTEST.recov), which are stored as BLOBs, one per test. LIST cannot handle
// BLOBs. Instead, we collect all tests and then aggregate them from the docs.
const aggregatePDTEST = (docs: Record<string, any>[]) => {
  const outputDocs: Record<string, any>[] = [];
  docs.forEach((inputDoc) => {
    const existingDoc = outputDocs.find(
      (outputDoc) => outputDoc.doc.well.wsn === inputDoc.doc.well.wsn
    );
    if (existingDoc) {
      existingDoc.doc.pdtest.push(inputDoc.doc.pdtest);
    } else {
      outputDocs.push({
        id: inputDoc.id,
        well_id: inputDoc.well_id,
        repo_id: inputDoc.repo_id,
        geo_type: inputDoc.geo_type,
        tag: inputDoc.tag,
        doc: {
          pdtest: [inputDoc.doc.pdtest],
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
    case "pdtest_treat":
      return (() => {
        try {
          const buf = Buffer.from(obj[key], "binary");
          const treatments = [];
          for (let i = 0; i < buf.length; i += 110) {
            let treat = {
              type: buf
                .subarray(i, i + 9)
                .toString()
                .split("\x00")[0],
              top: buf.subarray(i + 9, i + 17).readDoubleLE(),
              base: buf.subarray(i + 17, i + 25).readDoubleLE(),
              amount1: buf.subarray(i + 25, i + 33).readDoubleLE(),
              units1: buf
                .subarray(i + 61, i + 65)
                .toString()
                .split("\x00")[0],
              desc: buf
                .subarray(i + 68, i + 89)
                .toString()
                .split("\x00")[0],
              agent: buf
                .subarray(i + 89, i + 96)
                .toString()
                .split("\x00")[0],
              amount2: buf.subarray(i + 33, i + 41).readDoubleLE(),
              units2: buf
                .subarray(i + 96, i + 100)
                .toString()
                .split("\x00")[0],
              fmbrk: buf.subarray(i + 41, i + 50).readDoubleLE(),
              num_stages: buf.subarray(i + 57, i + 61).readInt32LE(),
              additive: buf
                .subarray(i + 103, i + 110)
                .toString()
                .split("\x00")[0],
              inj_rate: buf.subarray(i + 49, i + 57).readDoubleLE(),
            };
            treatments.push(treat);
          }
          return treatments;
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
  p_numtreat: {
    ts_type: "number",
  },
  p_flags: {
    ts_type: "number",
  },
  p_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  p_top: {
    ts_type: "number",
  },
  p_base: {
    ts_type: "number",
  },
  p_oilvol: {
    ts_type: "number",
  },
  p_gasvol: {
    ts_type: "number",
  },
  p_wtrvol: {
    ts_type: "number",
  },
  p_ftp: {
    ts_type: "number",
  },
  p_fcp: {
    ts_type: "number",
  },
  p_stp: {
    ts_type: "number",
  },
  p_scp: {
    ts_type: "number",
  },
  p_bht: {
    ts_type: "number",
  },
  p_bhp: {
    ts_type: "number",
  },
  p_choke: {
    ts_type: "number",
  },
  p_duration: {
    ts_type: "number",
  },
  p_caof: {
    ts_type: "number",
  },
  p_oilgty: {
    ts_type: "number",
  },
  p_gasgty: {
    ts_type: "number",
  },
  p_gor: {
    ts_type: "number",
  },
  p_testtype: {
    ts_type: "string",
  },
  p_fmname: {
    ts_type: "string",
  },
  p_oilunit: {
    ts_type: "string",
  },
  p_gasunit: {
    ts_type: "string",
  },
  p_wtrunit: {
    ts_type: "string",
  },
  p_remark: {
    ts_type: "number",
    xform: "memo_to_string",
  },
  p_treat: {
    ts_type: "string",
    xform: "pdtest_treat",
  },
  p_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  p_unitstype: {
    ts_type: "number",
  },
};

const prefixes = {
  w_: "well",
  p_: "pdtest",
};

const asset_id_keys = ["w_uwi", "p_recid"];

const well_id_keys = ["w_uwi"];

const default_chunk = 1000;

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    asset_id_keys: asset_id_keys,
    default_chunk: default_chunk,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    serialized_doc_processor: serialize(aggregatePDTEST),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
