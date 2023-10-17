import { serialize } from "https://deno.land/x/serialize_javascript/mod.ts";

const defineSQL = (filter) => {
  filter = filter ? filter : "";

  const where_clause_stub = "__pUrRwHeRe__";
  const idCols = ["w.wsn", "f.recid"];
  const idForm = idCols
    .map((i) => `CAST(${i} AS VARCHAR(10))`)
    .join(` || '-' || `);

  const where = filter.trim().length === 0 ? "" : `WHERE ${filter}`;

  let select = `SELECT
    w.wsn          AS w_wsn,
    w.uwi          AS w_uwi,

    f.recid        AS f_recid,
    f.wsn          AS f_wsn,
    f.numrecov     AS f_numrecov,
    f.nummts       AS f_nummts,
    f.flags        AS f_flags,
    f.date         AS f_date,
    f.top          AS f_top,
    f.base         AS f_base,
    f.ihp          AS f_ihp,
    f.fhp          AS f_fhp,
    f.ffp          AS f_ffp,
    f.isp          AS f_isp,
    f.fsp          AS f_fsp,
    f.bht          AS f_bht,
    f.bhp          AS f_bhp,
    f.choke        AS f_choke,
    f.cushamt      AS f_cushamt,
    f.testtype     AS f_testtype,
    f.fmname       AS f_fmname,
    f.cushtype     AS f_cushtype,
    f.ohtime       AS f_ohtime,
    f.sitime       AS f_sitime,
    f.remark       AS f_remark,
    f.recov        AS f_recov,
    f.mts          AS f_mts,
    f.chgdate      AS f_chgdate,
    f.unitstype    AS f_unitstype

  FROM well w
  JOIN fmtest f ON f.wsn = w.wsn AND f.testtype = 'D'
  ${where_clause_stub}
  `;

  const order = `ORDER BY w_uwi`;

  const identifier = `
    SELECT
      LIST(${idForm}) AS keylist
    FROM well w
    JOIN fmtest f ON f.wsn = w.wsn AND f.testtype = 'D'
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
const aggregateFMTEST = (docs: Record<string, any>[]) => {
  const outputDocs: Record<string, any>[] = [];
  docs.forEach((inputDoc) => {
    const existingDoc = outputDocs.find(
      (outputDoc) => outputDoc.doc.well.wsn === inputDoc.doc.well.wsn
    );
    if (existingDoc) {
      existingDoc.doc.fmtest.push(inputDoc.doc.fmtest);
    } else {
      outputDocs.push({
        id: inputDoc.id,
        well_id: inputDoc.well_id,
        repo_id: inputDoc.repo_id,
        geo_type: inputDoc.geo_type,
        tag: inputDoc.tag,
        doc: {
          fmtest: [inputDoc.doc.fmtest],
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
    case "fmtest_recov":
      return (() => {
        try {
          const buf = Buffer.from(obj[key], "binary");
          const recoveries = [];
          for (let i = 0; i < buf.length; i += 36) {
            let treat = {
              amount: buf.subarray(i, i + 8).readDoubleLE(),
              units: buf
                .subarray(i + 8, i + 15)
                .toString()
                .split("\x00")[0],
              descriptions: buf
                .subarray(i + 15, i + 36)
                .toString()
                .split("\x00")[0],
            };
            recoveries.push(treat);
          }
          return recoveries;
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

  // FMTEST

  f_recid: {
    ts_type: "number",
  },
  f_wsn: {
    ts_type: "number",
  },
  f_numrecov: {
    ts_type: "number",
  },
  f_nummts: {
    ts_type: "number",
  },
  f_flags: {
    ts_type: "number",
  },
  f_date: {
    ts_type: "date",
    xform: "excel_date",
  },
  f_top: {
    ts_type: "number",
  },
  f_base: {
    ts_type: "number",
  },

  f_ihp: {
    ts_type: "number",
  },
  f_fhp: {
    ts_type: "number",
  },
  f_ifp: {
    ts_type: "number",
  },
  f_ffp: {
    ts_type: "number",
  },
  f_isp: {
    ts_type: "number",
  },
  f_fsp: {
    ts_type: "number",
  },
  f_bht: {
    ts_type: "number",
  },
  f_bhp: {
    ts_type: "number",
  },
  f_choke: {
    ts_type: "number",
  },
  f_cushamt: {
    ts_type: "number",
  },
  f_testtype: {
    ts_type: "string",
  },
  f_fmname: {
    ts_type: "string",
  },
  f_cushtype: {
    ts_type: "string",
  },
  f_ohtime: {
    ts_type: "string",
  },
  f_sitime: {
    ts_type: "string",
  },
  f_remark: {
    ts_type: "number",
    xform: "memo_to_string",
  },
  f_recov: {
    ts_type: "string",
    xform: "fmtest_recov",
  },
  // TODO: decipher what mts BLOB contains, if anything
  f_mts: {
    ts_type: "string",
    xform: "memo_to_string",
  },
  f_chgdate: {
    ts_type: "date",
    xform: "excel_date",
  },
  f_unitstype: {
    ts_type: "number",
  },
};

const prefixes = {
  w_: "well",
  f_: "fmtest",
};

const asset_id_keys = ["w_uwi", "f_recid"];

const well_id_keys = ["w_uwi"];

const default_chunk = 100; // 1000

///////////////////////////////////////////////////////////////////////////////

export const getAssetDNA = (filter) => {
  return {
    asset_id_keys: asset_id_keys,
    default_chunk: default_chunk,
    prefixes: prefixes,
    serialized_xformer: serialize(xformer),
    serialized_doc_processor: serialize(aggregateFMTEST),
    sql: defineSQL(filter),
    well_id_keys: well_id_keys,
    xforms: xforms,
  };
};
